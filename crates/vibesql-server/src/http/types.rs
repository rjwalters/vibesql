//! HTTP API request and response types

use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use vibesql_types::SqlValue;

/// Query request for REST API
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryRequest {
    /// SQL query string
    pub sql: String,
    /// Query parameters (optional)
    #[serde(default)]
    pub params: Vec<JsonValue>,
}

impl QueryRequest {
    /// Convert JSON parameters to SqlValue
    pub fn to_sql_values(&self) -> Result<Vec<SqlValue>, String> {
        self.params.iter().map(json_to_sql_value).collect()
    }
}

/// Query response for REST API
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResponse {
    /// Column names
    pub columns: Vec<String>,
    /// Result rows with values
    pub rows: Vec<Vec<JsonValue>>,
    /// Number of rows returned
    pub row_count: usize,
}

/// Mutation response (INSERT, UPDATE, DELETE)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MutationResponse {
    /// Number of rows affected
    pub rows_affected: usize,
}

/// Schema information for a table
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableInfo {
    /// Table name
    pub name: String,
    /// Column definitions
    pub columns: Vec<ColumnInfo>,
}

/// Column schema information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    /// Column name
    pub name: String,
    /// SQL data type
    pub data_type: String,
    /// Whether column is nullable
    pub nullable: bool,
    /// Whether column is primary key
    pub primary_key: bool,
}

/// Error response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorResponse {
    /// Error message
    pub error: String,
    /// Optional error code
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
}

impl ErrorResponse {
    pub fn new(error: impl Into<String>) -> Self {
        Self { error: error.into(), code: None }
    }

    pub fn with_code(error: impl Into<String>, code: impl Into<String>) -> Self {
        Self { error: error.into(), code: Some(code.into()) }
    }
}

/// Health check response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
}

/// Convert SqlValue to JSON
pub fn sql_value_to_json(val: &SqlValue) -> JsonValue {
    match val {
        SqlValue::Null => JsonValue::Null,
        SqlValue::Boolean(b) => JsonValue::Bool(*b),
        SqlValue::Integer(i) => json!(*i),
        SqlValue::Smallint(i) => json!(*i as i64),
        SqlValue::Bigint(i) => json!(*i),
        SqlValue::Unsigned(u) => json!(*u),
        SqlValue::Numeric(f) => {
            if f.is_nan() || f.is_infinite() {
                JsonValue::Null
            } else {
                json!(*f)
            }
        }
        SqlValue::Float(f) => {
            if f.is_nan() || f.is_infinite() {
                JsonValue::Null
            } else {
                json!(*f as f64)
            }
        }
        SqlValue::Real(f) => {
            if f.is_nan() || f.is_infinite() {
                JsonValue::Null
            } else {
                json!(*f as f64)
            }
        }
        SqlValue::Double(f) => {
            if f.is_nan() || f.is_infinite() {
                JsonValue::Null
            } else {
                json!(*f)
            }
        }
        SqlValue::Character(s) | SqlValue::Varchar(s) => JsonValue::String(s.clone()),
        SqlValue::Timestamp(ts) => JsonValue::String(format!("{:?}", ts)),
        SqlValue::Date(d) => JsonValue::String(format!("{:?}", d)),
        SqlValue::Time(t) => JsonValue::String(format!("{:?}", t)),
        SqlValue::Interval(_) => JsonValue::Null, // TODO: proper interval serialization
        SqlValue::Vector(v) => json!(v), // Vector as JSON array of floats
    }
}

/// Convert JSON to SqlValue
pub fn json_to_sql_value(val: &JsonValue) -> Result<SqlValue, String> {
    match val {
        JsonValue::Null => Ok(SqlValue::Null),
        JsonValue::Bool(b) => Ok(SqlValue::Boolean(*b)),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(SqlValue::Integer(i))
            } else if let Some(f) = n.as_f64() {
                Ok(SqlValue::Numeric(f))
            } else {
                Err("Invalid number".to_string())
            }
        }
        JsonValue::String(s) => Ok(SqlValue::Varchar(s.clone())),
        JsonValue::Array(_) => Err("Arrays not yet supported".to_string()),
        JsonValue::Object(_) => Err("Objects not yet supported".to_string()),
    }
}
