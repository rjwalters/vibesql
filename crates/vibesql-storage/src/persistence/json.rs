// ============================================================================
// JSON Format Support (Save/Load Operations)
// ============================================================================
//
// Provides JSON serialization/deserialization for database persistence:
// - Human-readable, tool-friendly format
// - Standard JSON for easy integration with other systems
// - Supports all SQL types with proper representation

use std::{
    collections::HashMap,
    fs::File,
    io::{BufReader, BufWriter, Write},
    path::Path,
};

use serde::{Deserialize, Serialize};
use vibesql_ast::OrderDirection;
use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_types::{DataType, SqlValue};

use crate::{Database, Row, StorageError, Table};

// ============================================================================
// JSON Schema Structures
// ============================================================================

/// Root JSON document structure
#[derive(Debug, Serialize, Deserialize)]
pub struct JsonDatabase {
    /// Metadata about the format
    pub vibesql: JsonMetadata,
    /// Database schemas (excluding 'public' which is implicit)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub schemas: Vec<JsonSchema>,
    /// Database roles
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub roles: Vec<JsonRole>,
    /// Tables with data
    pub tables: Vec<JsonTable>,
    /// User-defined indexes
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub indexes: Vec<JsonIndex>,
    /// Views
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub views: Vec<JsonView>,
}

/// Format metadata
#[derive(Debug, Serialize, Deserialize)]
pub struct JsonMetadata {
    /// Format version (for future compatibility)
    pub version: String,
    /// Format type
    pub format: String,
    /// Timestamp when exported
    #[serde(with = "chrono::serde::ts_seconds")]
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

/// Schema representation
#[derive(Debug, Serialize, Deserialize)]
pub struct JsonSchema {
    pub name: String,
}

/// Role representation
#[derive(Debug, Serialize, Deserialize)]
pub struct JsonRole {
    pub name: String,
}

/// Table with schema and data
#[derive(Debug, Serialize, Deserialize)]
pub struct JsonTable {
    pub name: String,
    #[serde(default = "default_schema_name")]
    pub schema: String,
    pub columns: Vec<JsonColumn>,
    #[serde(default)]
    pub rows: Vec<JsonRow>,
}

fn default_schema_name() -> String {
    "public".to_string()
}

/// Column schema
#[derive(Debug, Serialize, Deserialize)]
pub struct JsonColumn {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: String,
    #[serde(default)]
    pub nullable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_length: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub precision: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scale: Option<u8>,
}

/// Row data (key-value pairs for readability)
pub type JsonRow = HashMap<String, serde_json::Value>;

/// Index representation
#[derive(Debug, Serialize, Deserialize)]
pub struct JsonIndex {
    pub name: String,
    pub table: String,
    pub columns: Vec<JsonIndexColumn>,
    #[serde(default)]
    pub unique: bool,
}

/// Index column with sort direction
#[derive(Debug, Serialize, Deserialize)]
pub struct JsonIndexColumn {
    pub name: String,
    #[serde(default = "default_asc")]
    pub direction: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix_length: Option<u64>,
}

fn default_asc() -> String {
    "ASC".to_string()
}

/// View representation
#[derive(Debug, Serialize, Deserialize)]
pub struct JsonView {
    pub name: String,
    pub definition: String,
}

// ============================================================================
// Options for JSON serialization
// ============================================================================

/// Configuration options for JSON export
#[derive(Debug, Clone)]
pub struct JsonOptions {
    /// Pretty-print the JSON (true) or minified (false)
    pub pretty: bool,
    /// Include metadata (timestamp, version)
    pub include_metadata: bool,
}

impl Default for JsonOptions {
    fn default() -> Self {
        JsonOptions { pretty: true, include_metadata: true }
    }
}

// ============================================================================
// Database save_json implementation
// ============================================================================

impl Database {
    /// Save database in JSON format with default options
    ///
    /// # Example
    /// ```no_run
    /// # use vibesql_storage::Database;
    /// let db = Database::new();
    /// db.save_json("database.json").unwrap();
    /// ```
    pub fn save_json<P: AsRef<Path>>(&self, path: P) -> Result<(), StorageError> {
        self.save_json_with_options(path, JsonOptions::default())
    }

    /// Save database in JSON format with custom options
    ///
    /// # Example
    /// ```no_run
    /// # use vibesql_storage::Database;
    /// # use vibesql_storage::persistence::json::JsonOptions;
    /// let db = Database::new();
    /// let options = JsonOptions { pretty: true, include_metadata: true };
    /// db.save_json_with_options("database.json", options).unwrap();
    /// ```
    pub fn save_json_with_options<P: AsRef<Path>>(
        &self,
        path: P,
        options: JsonOptions,
    ) -> Result<(), StorageError> {
        let file = File::create(path)
            .map_err(|e| StorageError::NotImplemented(format!("Failed to create file: {}", e)))?;

        let mut writer = BufWriter::new(file);

        // Build JSON structure
        let json_db = self.to_json_database(options.include_metadata)?;

        // Serialize to JSON
        let json_str = if options.pretty {
            serde_json::to_string_pretty(&json_db)
        } else {
            serde_json::to_string(&json_db)
        }
        .map_err(|e| StorageError::NotImplemented(format!("JSON serialization failed: {}", e)))?;

        // Write to file
        writer
            .write_all(json_str.as_bytes())
            .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;

        writer.flush().map_err(|e| StorageError::NotImplemented(format!("Flush error: {}", e)))?;

        Ok(())
    }

    /// Convert Database to JSON representation
    fn to_json_database(&self, include_metadata: bool) -> Result<JsonDatabase, StorageError> {
        // Metadata
        let vibesql = if include_metadata {
            JsonMetadata {
                version: "1.0".to_string(),
                format: "json".to_string(),
                timestamp: chrono::Utc::now(),
            }
        } else {
            JsonMetadata {
                version: "1.0".to_string(),
                format: "json".to_string(),
                timestamp: chrono::DateTime::from_timestamp(0, 0).unwrap(),
            }
        };

        // Schemas (excluding default 'public')
        let schemas = self
            .catalog
            .list_schemas()
            .into_iter()
            .filter(|name| name != "public")
            .map(|name| JsonSchema { name })
            .collect();

        // Roles
        let roles = self.catalog.list_roles().into_iter().map(|name| JsonRole { name }).collect();

        // Tables with data
        let mut tables = Vec::new();
        for table_name in self.catalog.list_tables() {
            if let Some(table) = self.get_table(&table_name) {
                tables.push(table_to_json(&table_name, table)?);
            }
        }

        // Indexes
        let indexes = self
            .list_indexes()
            .into_iter()
            .filter_map(|index_name| {
                self.get_index(&index_name).map(|metadata| JsonIndex {
                    name: index_name,
                    table: metadata.table_name.clone(),
                    columns: metadata
                        .columns
                        .iter()
                        .map(|col| JsonIndexColumn {
                            name: col.column_name.clone(),
                            direction: match col.direction {
                                OrderDirection::Desc => "DESC".to_string(),
                                OrderDirection::Asc => "ASC".to_string(),
                            },
                            prefix_length: col.prefix_length,
                        })
                        .collect(),
                    unique: metadata.unique,
                })
            })
            .collect();

        // Views - serialize view definitions
        let views = self
            .catalog
            .list_views()
            .into_iter()
            .filter_map(|view_name| {
                self.catalog.get_view(&view_name).map(|view_def| {
                    let definition = view_def
                        .sql_definition
                        .clone()
                        .unwrap_or_else(|| format!("{:#?}", view_def.query));
                    JsonView { name: view_name, definition }
                })
            })
            .collect();

        Ok(JsonDatabase { vibesql, schemas, roles, tables, indexes, views })
    }

    /// Load database from JSON format
    ///
    /// # Example
    /// ```no_run
    /// # use vibesql_storage::Database;
    /// let db = Database::load_json("database.json").unwrap();
    /// ```
    pub fn load_json<P: AsRef<Path>>(path: P) -> Result<Self, StorageError> {
        let file = File::open(path)
            .map_err(|e| StorageError::NotImplemented(format!("Failed to open file: {}", e)))?;

        let reader = BufReader::new(file);

        // Deserialize from JSON
        let json_db: JsonDatabase = serde_json::from_reader(reader).map_err(|e| {
            StorageError::NotImplemented(format!("JSON deserialization failed: {}", e))
        })?;

        // Convert to Database
        json_database_to_db(json_db)
    }
}

// ============================================================================
// Conversion functions
// ============================================================================

/// Convert Table to JSON representation
fn table_to_json(table_name: &str, table: &Table) -> Result<JsonTable, StorageError> {
    let schema_name = if let Some(idx) = table_name.find('.') {
        table_name[..idx].to_string()
    } else {
        "public".to_string()
    };

    let unqualified_name = if let Some(idx) = table_name.find('.') {
        table_name[idx + 1..].to_string()
    } else {
        table_name.to_string()
    };

    let columns = table.schema.columns.iter().map(column_to_json).collect();

    let rows = table
        .scan()
        .iter()
        .map(|row| {
            let mut row_map = HashMap::new();
            for (i, value) in row.values.iter().enumerate() {
                let col_name = &table.schema.columns[i].name;
                row_map.insert(col_name.clone(), sql_value_to_json(value));
            }
            row_map
        })
        .collect();

    Ok(JsonTable { name: unqualified_name, schema: schema_name, columns, rows })
}

/// Convert ColumnSchema to JSON representation
fn column_to_json(col: &ColumnSchema) -> JsonColumn {
    let (type_name, max_length, precision, scale) = match &col.data_type {
        DataType::Integer => ("INTEGER".to_string(), None, None, None),
        DataType::Smallint => ("SMALLINT".to_string(), None, None, None),
        DataType::Bigint => ("BIGINT".to_string(), None, None, None),
        DataType::Unsigned => ("UNSIGNED".to_string(), None, None, None),
        DataType::Numeric { precision: p, scale: s } => {
            ("NUMERIC".to_string(), None, Some(*p), Some(*s))
        }
        DataType::Decimal { precision: p, scale: s } => {
            ("DECIMAL".to_string(), None, Some(*p), Some(*s))
        }
        DataType::Float { precision: p } => ("FLOAT".to_string(), None, Some(*p), None),
        DataType::Real => ("REAL".to_string(), None, None, None),
        DataType::DoublePrecision => ("DOUBLE PRECISION".to_string(), None, None, None),
        DataType::Character { length } => ("CHAR".to_string(), Some(*length), None, None),
        DataType::Varchar { max_length: ml } => ("VARCHAR".to_string(), *ml, None, None),
        DataType::CharacterLargeObject => ("CLOB".to_string(), None, None, None),
        DataType::Name => ("NAME".to_string(), None, None, None),
        DataType::Boolean => ("BOOLEAN".to_string(), None, None, None),
        DataType::Date => ("DATE".to_string(), None, None, None),
        DataType::Time { with_timezone } => {
            if *with_timezone {
                ("TIME WITH TIME ZONE".to_string(), None, None, None)
            } else {
                ("TIME".to_string(), None, None, None)
            }
        }
        DataType::Timestamp { with_timezone } => {
            // NOTE: DATETIME is serialized as TIMESTAMP since they are
            // internally represented as the same type (DataType::Timestamp).
            // This is intentional - see issue #1626 for rationale.
            if *with_timezone {
                ("TIMESTAMP WITH TIME ZONE".to_string(), None, None, None)
            } else {
                ("TIMESTAMP".to_string(), None, None, None)
            }
        }
        DataType::Interval { .. } => ("INTERVAL".to_string(), None, None, None),
        DataType::BinaryLargeObject => ("BLOB".to_string(), None, None, None),
        DataType::Bit { length } => ("BIT".to_string(), *length, None, None),
        DataType::Vector { dimensions } => (format!("VECTOR({})", dimensions), None, None, None),
        DataType::UserDefined { type_name } => (type_name.clone(), None, None, None),
        DataType::Null => ("NULL".to_string(), None, None, None),
    };

    JsonColumn {
        name: col.name.clone(),
        data_type: type_name,
        nullable: col.nullable,
        max_length,
        precision,
        scale,
    }
}

/// Convert SqlValue to JSON representation
fn sql_value_to_json(value: &SqlValue) -> serde_json::Value {
    match value {
        SqlValue::Integer(v) => serde_json::Value::Number((*v).into()),
        SqlValue::Smallint(v) => serde_json::Value::Number((*v).into()),
        SqlValue::Bigint(v) => serde_json::Value::Number((*v).into()),
        SqlValue::Unsigned(v) => serde_json::Value::Number((*v).into()),
        SqlValue::Numeric(v) => serde_json::json!(v),
        SqlValue::Float(v) => serde_json::json!(v),
        SqlValue::Real(v) => serde_json::json!(v),
        SqlValue::Double(v) => serde_json::json!(v),
        SqlValue::Character(s) | SqlValue::Varchar(s) => serde_json::Value::String(s.to_string()),
        SqlValue::Boolean(b) => serde_json::Value::Bool(*b),
        SqlValue::Date(d) => serde_json::Value::String(d.to_string()),
        SqlValue::Time(t) => serde_json::Value::String(t.to_string()),
        SqlValue::Timestamp(ts) => serde_json::Value::String(ts.to_string()),
        SqlValue::Interval(i) => serde_json::Value::String(i.to_string()),
        SqlValue::Vector(v) => {
            // Serialize vector as JSON array of floats
            serde_json::Value::Array(v.iter().map(|f| serde_json::json!(f)).collect())
        }
        SqlValue::Null => serde_json::Value::Null,
    }
}

/// Convert JsonDatabase to Database
fn json_database_to_db(json_db: JsonDatabase) -> Result<Database, StorageError> {
    let mut db = Database::new();

    // Create schemas
    for schema in json_db.schemas {
        db.catalog.create_schema(schema.name).map_err(|e: vibesql_catalog::CatalogError| {
            StorageError::CatalogError(e.to_string())
        })?;
    }

    // Create roles
    for role in json_db.roles {
        db.catalog.create_role(role.name).map_err(|e: vibesql_catalog::CatalogError| {
            StorageError::CatalogError(e.to_string())
        })?;
    }

    // Create tables and insert data
    for json_table in json_db.tables {
        // Build table schema
        let columns: Vec<ColumnSchema> =
            json_table.columns.iter().map(json_column_to_schema).collect::<Result<Vec<_>, _>>()?;

        let table_schema = TableSchema::new(json_table.name.clone(), columns);

        // Create table - Database::create_table() automatically qualifies with default schema
        db.create_table(table_schema.clone())?;

        // Insert rows using qualified table name
        if !json_table.rows.is_empty() {
            let qualified_name = format!("{}.{}", json_table.schema, json_table.name);
            if let Some(table) = db.get_table_mut(&qualified_name) {
                for json_row in json_table.rows {
                    let row = json_row_to_row(&json_row, &table_schema)?;
                    table.insert(row)?;
                }
            }
        }
    }

    // Views are preserved in JSON export but not automatically recreated during import.
    // This is intentional - recreating views would require SQL parsing and execution
    // during deserialization, which could fail or have side effects.
    // Views should be recreated manually or via SQL execution after loading.
    if !json_db.views.is_empty() {
        log::warn!(
            "{} view(s) found in JSON export but not automatically recreated. \
             Please recreate views manually using CREATE VIEW statements.",
            json_db.views.len()
        );
    }

    // Create indexes
    for json_index in json_db.indexes {
        let columns: Vec<vibesql_ast::IndexColumn> = json_index
            .columns
            .iter()
            .map(|c| vibesql_ast::IndexColumn {
                column_name: c.name.clone(),
                direction: if c.direction == "DESC" {
                    OrderDirection::Desc
                } else {
                    OrderDirection::Asc
                },
                prefix_length: None,
            })
            .collect();

        db.create_index(json_index.name, json_index.table, json_index.unique, columns)?;
    }

    Ok(db)
}

/// Convert JsonColumn to ColumnSchema
fn json_column_to_schema(col: &JsonColumn) -> Result<ColumnSchema, StorageError> {
    let data_type = parse_data_type(&col.data_type, col.max_length, col.precision, col.scale)?;
    Ok(ColumnSchema {
        name: col.name.clone(),
        data_type,
        nullable: col.nullable,
        default_value: None,
    })
}

/// Parse data type from string
fn parse_data_type(
    type_str: &str,
    max_length: Option<usize>,
    precision: Option<u8>,
    scale: Option<u8>,
) -> Result<DataType, StorageError> {
    match type_str.to_uppercase().as_str() {
        "INTEGER" | "INT" => Ok(DataType::Integer),
        "SMALLINT" => Ok(DataType::Smallint),
        "BIGINT" => Ok(DataType::Bigint),
        "UNSIGNED" => Ok(DataType::Unsigned),
        "NUMERIC" => {
            Ok(DataType::Numeric { precision: precision.unwrap_or(38), scale: scale.unwrap_or(0) })
        }
        "DECIMAL" => {
            Ok(DataType::Decimal { precision: precision.unwrap_or(38), scale: scale.unwrap_or(0) })
        }
        "FLOAT" => Ok(DataType::Float { precision: precision.unwrap_or(53) }),
        "REAL" => Ok(DataType::Real),
        "DOUBLE PRECISION" | "DOUBLE" => Ok(DataType::DoublePrecision),
        "CHAR" | "CHARACTER" => Ok(DataType::Character { length: max_length.unwrap_or(1) }),
        "VARCHAR" => Ok(DataType::Varchar { max_length }),
        "CLOB" => Ok(DataType::CharacterLargeObject),
        "NAME" => Ok(DataType::Name),
        "BOOLEAN" | "BOOL" => Ok(DataType::Boolean),
        "DATE" => Ok(DataType::Date),
        "TIME" => Ok(DataType::Time { with_timezone: false }),
        "TIME WITH TIME ZONE" => Ok(DataType::Time { with_timezone: true }),
        "TIMESTAMP" | "DATETIME" => Ok(DataType::Timestamp { with_timezone: false }),
        "TIMESTAMP WITH TIME ZONE" | "DATETIME WITH TIME ZONE" => {
            Ok(DataType::Timestamp { with_timezone: true })
        }
        "INTERVAL" => Ok(DataType::Interval {
            start_field: vibesql_types::IntervalField::Day,
            end_field: None,
        }),
        "BLOB" => Ok(DataType::BinaryLargeObject),
        "NULL" => Ok(DataType::Null),
        other => Ok(DataType::UserDefined { type_name: other.to_string() }),
    }
}

/// Convert JsonRow to Row
fn json_row_to_row(json_row: &JsonRow, schema: &TableSchema) -> Result<Row, StorageError> {
    let mut values = Vec::new();
    for col in &schema.columns {
        let json_value = json_row.get(&col.name).ok_or_else(|| {
            StorageError::NotImplemented(format!("Missing column '{}' in JSON row", col.name))
        })?;
        values.push(json_value_to_sql(json_value, &col.data_type)?);
    }
    Ok(Row::new(values))
}

/// Convert JSON value to SqlValue
fn json_value_to_sql(
    json_value: &serde_json::Value,
    data_type: &DataType,
) -> Result<SqlValue, StorageError> {
    match (json_value, data_type) {
        (serde_json::Value::Null, _) => Ok(SqlValue::Null),
        (serde_json::Value::Number(n), DataType::Integer) => n
            .as_i64()
            .map(SqlValue::Integer)
            .ok_or_else(|| StorageError::NotImplemented(format!("Invalid integer value: {}", n))),
        (serde_json::Value::Number(n), DataType::Smallint) => {
            n.as_i64().and_then(|v| i16::try_from(v).ok()).map(SqlValue::Smallint).ok_or_else(
                || StorageError::NotImplemented(format!("Invalid smallint value: {}", n)),
            )
        }
        (serde_json::Value::Number(n), DataType::Bigint) => n
            .as_i64()
            .map(SqlValue::Bigint)
            .ok_or_else(|| StorageError::NotImplemented(format!("Invalid bigint value: {}", n))),
        (serde_json::Value::Number(n), DataType::Unsigned) => n
            .as_u64()
            .map(SqlValue::Unsigned)
            .ok_or_else(|| StorageError::NotImplemented(format!("Invalid unsigned value: {}", n))),
        (serde_json::Value::Number(n), DataType::Numeric { .. })
        | (serde_json::Value::Number(n), DataType::Decimal { .. }) => n
            .as_f64()
            .map(SqlValue::Numeric)
            .ok_or_else(|| StorageError::NotImplemented(format!("Invalid numeric value: {}", n))),
        (serde_json::Value::Number(n), DataType::Float { .. }) => n
            .as_f64()
            .map(|v| SqlValue::Float(v as f32))
            .ok_or_else(|| StorageError::NotImplemented(format!("Invalid float value: {}", n))),
        (serde_json::Value::Number(n), DataType::Real) => n
            .as_f64()
            .map(|v| SqlValue::Real(v as f32))
            .ok_or_else(|| StorageError::NotImplemented(format!("Invalid real value: {}", n))),
        (serde_json::Value::Number(n), DataType::DoublePrecision) => n
            .as_f64()
            .map(SqlValue::Double)
            .ok_or_else(|| StorageError::NotImplemented(format!("Invalid double value: {}", n))),
        (serde_json::Value::String(s), DataType::Character { .. }) => {
            Ok(SqlValue::Character(arcstr::ArcStr::from(s.as_str())))
        }
        (serde_json::Value::String(s), DataType::Varchar { .. })
        | (serde_json::Value::String(s), DataType::Name) => Ok(SqlValue::Varchar(arcstr::ArcStr::from(s.as_str()))),
        (serde_json::Value::Bool(b), DataType::Boolean) => Ok(SqlValue::Boolean(*b)),
        (serde_json::Value::String(s), DataType::Date) => s
            .parse()
            .map(SqlValue::Date)
            .map_err(|e| StorageError::NotImplemented(format!("Invalid date: {}", e))),
        (serde_json::Value::String(s), DataType::Time { .. }) => s
            .parse()
            .map(SqlValue::Time)
            .map_err(|e| StorageError::NotImplemented(format!("Invalid time: {}", e))),
        (serde_json::Value::String(s), DataType::Timestamp { .. }) => s
            .parse()
            .map(SqlValue::Timestamp)
            .map_err(|e| StorageError::NotImplemented(format!("Invalid timestamp: {}", e))),
        (serde_json::Value::String(s), DataType::Interval { .. }) => s
            .parse()
            .map(SqlValue::Interval)
            .map_err(|e| StorageError::NotImplemented(format!("Invalid interval: {}", e))),
        _ => Err(StorageError::NotImplemented(format!(
            "Unsupported JSON value {:?} for type {:?}",
            json_value, data_type
        ))),
    }
}
