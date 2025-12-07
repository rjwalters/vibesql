//! Auto-generated CRUD endpoints for VibeSQL HTTP REST API
//!
//! This module provides RESTful CRUD endpoints that automatically expose
//! database tables as REST resources without requiring custom code.
//!
//! ## API Design
//!
//! ```text
//! GET    /api/tables/{table}/rows          # SELECT * FROM {table}
//! GET    /api/tables/{table}/rows/{id}     # SELECT * FROM {table} WHERE pk = {id}
//! POST   /api/tables/{table}/rows          # INSERT INTO {table} ...
//! PUT    /api/tables/{table}/rows/{id}     # Full UPDATE (all columns)
//! PATCH  /api/tables/{table}/rows/{id}     # Partial UPDATE (specified columns only)
//! DELETE /api/tables/{table}/rows/{id}     # DELETE FROM {table} WHERE pk = {id}
//! ```
//!
//! ## Query Parameters (GET collection)
//!
//! ```text
//! GET /api/tables/users/rows?select=id,name&order=created_at.desc&limit=10&offset=0
//! GET /api/tables/users/rows?name=eq.Alice
//! GET /api/tables/users/rows?age=gt.21&status=in.(active,pending)
//! ```
//!
//! ## Filter Operators
//!
//! - `eq` - Equal to
//! - `neq` - Not equal to
//! - `gt` - Greater than
//! - `gte` - Greater than or equal
//! - `lt` - Less than
//! - `lte` - Less than or equal
//! - `like` - SQL LIKE pattern
//! - `ilike` - Case-insensitive LIKE
//! - `in` - In list: `status=in.(active,pending)`
//! - `is` - IS NULL/NOT NULL: `deleted_at=is.null`

use std::collections::HashMap;

use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use tracing::{debug, error};

use super::rest::{get_database_name, HttpState};
use super::types::*;
use crate::registry::SharedDatabase;

/// Query parameters for GET collection endpoint
#[derive(Debug, Deserialize, Default)]
pub struct CrudQueryParams {
    /// Columns to select (comma-separated)
    #[serde(default)]
    pub select: Option<String>,
    /// Column to order by (format: column.asc or column.desc)
    #[serde(default)]
    pub order: Option<String>,
    /// Maximum number of rows to return
    #[serde(default)]
    pub limit: Option<u32>,
    /// Number of rows to skip
    #[serde(default)]
    pub offset: Option<u32>,
    /// Additional filter parameters (dynamic key-value pairs)
    #[serde(flatten)]
    pub filters: HashMap<String, String>,
}

/// Request body for POST (create) endpoint
#[derive(Debug, Deserialize)]
pub struct CreateRequest {
    /// Column values as key-value pairs
    #[serde(flatten)]
    pub values: HashMap<String, JsonValue>,
}

/// Request body for PUT (full update) endpoint
#[derive(Debug, Deserialize)]
pub struct UpdateRequest {
    /// Column values as key-value pairs (all columns required)
    #[serde(flatten)]
    pub values: HashMap<String, JsonValue>,
}

/// Request body for PATCH (partial update) endpoint
#[derive(Debug, Deserialize)]
pub struct PatchRequest {
    /// Column values to update (only specified columns)
    #[serde(flatten)]
    pub values: HashMap<String, JsonValue>,
}

/// Response for single resource operations
#[derive(Debug, Serialize)]
pub struct ResourceResponse {
    /// The resource data
    pub data: HashMap<String, JsonValue>,
}

/// Response for collection operations
#[derive(Debug, Serialize)]
pub struct CollectionResponse {
    /// Array of resources
    pub data: Vec<HashMap<String, JsonValue>>,
    /// Total count (if available)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total: Option<usize>,
}

/// Filter operator parsed from query parameter value
#[derive(Debug, Clone)]
pub enum FilterOperator {
    Eq(String),
    Neq(String),
    Gt(String),
    Gte(String),
    Lt(String),
    Lte(String),
    Like(String),
    Ilike(String),
    In(Vec<String>),
    Is(IsValue),
}

#[derive(Debug, Clone)]
pub enum IsValue {
    Null,
    NotNull,
    True,
    False,
}

impl FilterOperator {
    /// Parse a filter value like "eq.value" or "in.(a,b,c)"
    pub fn parse(value: &str) -> Result<Self, String> {
        // Find the operator prefix
        if let Some((op, val)) = value.split_once('.') {
            match op {
                "eq" => Ok(FilterOperator::Eq(val.to_string())),
                "neq" => Ok(FilterOperator::Neq(val.to_string())),
                "gt" => Ok(FilterOperator::Gt(val.to_string())),
                "gte" => Ok(FilterOperator::Gte(val.to_string())),
                "lt" => Ok(FilterOperator::Lt(val.to_string())),
                "lte" => Ok(FilterOperator::Lte(val.to_string())),
                "like" => Ok(FilterOperator::Like(val.to_string())),
                "ilike" => Ok(FilterOperator::Ilike(val.to_string())),
                "in" => {
                    // Parse (a,b,c) format
                    let val = val.trim();
                    if val.starts_with('(') && val.ends_with(')') {
                        let inner = &val[1..val.len() - 1];
                        let items: Vec<String> =
                            inner.split(',').map(|s| s.trim().to_string()).collect();
                        Ok(FilterOperator::In(items))
                    } else {
                        Err(format!("Invalid IN format: {}. Expected in.(a,b,c)", value))
                    }
                }
                "is" => {
                    let val_lower = val.to_lowercase();
                    match val_lower.as_str() {
                        "null" => Ok(FilterOperator::Is(IsValue::Null)),
                        "notnull" | "not_null" => Ok(FilterOperator::Is(IsValue::NotNull)),
                        "true" => Ok(FilterOperator::Is(IsValue::True)),
                        "false" => Ok(FilterOperator::Is(IsValue::False)),
                        _ => Err(format!(
                            "Invalid IS value: {}. Expected null, notnull, true, or false",
                            val
                        )),
                    }
                }
                _ => Err(format!("Unknown filter operator: {}", op)),
            }
        } else {
            // No operator prefix - treat as equality
            Ok(FilterOperator::Eq(value.to_string()))
        }
    }

    /// Convert to SQL condition string
    pub fn to_sql_condition(&self, column: &str) -> String {
        match self {
            FilterOperator::Eq(val) => format!("\"{}\" = '{}'", column, escape_sql_string(val)),
            FilterOperator::Neq(val) => format!("\"{}\" <> '{}'", column, escape_sql_string(val)),
            FilterOperator::Gt(val) => format!("\"{}\" > '{}'", column, escape_sql_string(val)),
            FilterOperator::Gte(val) => format!("\"{}\" >= '{}'", column, escape_sql_string(val)),
            FilterOperator::Lt(val) => format!("\"{}\" < '{}'", column, escape_sql_string(val)),
            FilterOperator::Lte(val) => format!("\"{}\" <= '{}'", column, escape_sql_string(val)),
            FilterOperator::Like(val) => {
                format!("\"{}\" LIKE '{}'", column, escape_sql_string(val))
            }
            FilterOperator::Ilike(val) => {
                format!("LOWER(\"{}\") LIKE LOWER('{}')", column, escape_sql_string(val))
            }
            FilterOperator::In(vals) => {
                let list = vals
                    .iter()
                    .map(|v| format!("'{}'", escape_sql_string(v)))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("\"{}\" IN ({})", column, list)
            }
            FilterOperator::Is(is_val) => match is_val {
                IsValue::Null => format!("\"{}\" IS NULL", column),
                IsValue::NotNull => format!("\"{}\" IS NOT NULL", column),
                IsValue::True => format!("\"{}\" = TRUE", column),
                IsValue::False => format!("\"{}\" = FALSE", column),
            },
        }
    }
}

/// Escape single quotes in SQL strings
fn escape_sql_string(s: &str) -> String {
    s.replace('\'', "''")
}

/// Build SELECT SQL from query parameters
fn build_select_sql(table_name: &str, params: &CrudQueryParams) -> String {
    let mut sql = String::new();

    // SELECT clause
    let columns = match &params.select {
        Some(cols) => {
            cols.split(',').map(|c| format!("\"{}\"", c.trim())).collect::<Vec<_>>().join(", ")
        }
        None => "*".to_string(),
    };
    sql.push_str(&format!("SELECT {} FROM \"{}\"", columns, table_name));

    // WHERE clause from filters
    let reserved_params = ["select", "order", "limit", "offset"];
    let conditions: Vec<String> = params
        .filters
        .iter()
        .filter(|(k, _)| !reserved_params.contains(&k.as_str()))
        .filter_map(|(col, val)| {
            match FilterOperator::parse(val) {
                Ok(op) => Some(op.to_sql_condition(col)),
                Err(_) => None, // Skip invalid filters
            }
        })
        .collect();

    if !conditions.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&conditions.join(" AND "));
    }

    // ORDER BY clause
    if let Some(order) = &params.order {
        let order_parts: Vec<String> = order
            .split(',')
            .map(|part| {
                let part = part.trim();
                if let Some((col, dir)) = part.rsplit_once('.') {
                    let dir_sql = if dir.eq_ignore_ascii_case("desc") { "DESC" } else { "ASC" };
                    format!("\"{}\" {}", col, dir_sql)
                } else {
                    format!("\"{}\" ASC", part)
                }
            })
            .collect();
        sql.push_str(&format!(" ORDER BY {}", order_parts.join(", ")));
    }

    // LIMIT clause
    if let Some(limit) = params.limit {
        sql.push_str(&format!(" LIMIT {}", limit));
    }

    // OFFSET clause
    if let Some(offset) = params.offset {
        sql.push_str(&format!(" OFFSET {}", offset));
    }

    sql
}

/// Build SELECT SQL for a single resource by primary key
fn build_select_by_pk_sql(
    table_name: &str,
    pk_column: &str,
    pk_value: &str,
    columns: Option<&str>,
) -> String {
    let select_cols = match columns {
        Some(cols) => {
            cols.split(',').map(|c| format!("\"{}\"", c.trim())).collect::<Vec<_>>().join(", ")
        }
        None => "*".to_string(),
    };
    format!(
        "SELECT {} FROM \"{}\" WHERE \"{}\" = '{}'",
        select_cols,
        table_name,
        pk_column,
        escape_sql_string(pk_value)
    )
}

/// Build INSERT SQL from values
fn build_insert_sql(table_name: &str, values: &HashMap<String, JsonValue>) -> String {
    let columns: Vec<String> = values.keys().map(|k| format!("\"{}\"", k)).collect();
    let vals: Vec<String> = values.values().map(json_to_sql_literal).collect();

    format!("INSERT INTO \"{}\" ({}) VALUES ({})", table_name, columns.join(", "), vals.join(", "))
}

/// Build UPDATE SQL from values
fn build_update_sql(
    table_name: &str,
    pk_column: &str,
    pk_value: &str,
    values: &HashMap<String, JsonValue>,
) -> String {
    let set_clauses: Vec<String> = values
        .iter()
        .map(|(col, val)| format!("\"{}\" = {}", col, json_to_sql_literal(val)))
        .collect();

    format!(
        "UPDATE \"{}\" SET {} WHERE \"{}\" = '{}'",
        table_name,
        set_clauses.join(", "),
        pk_column,
        escape_sql_string(pk_value)
    )
}

/// Build DELETE SQL
fn build_delete_sql(table_name: &str, pk_column: &str, pk_value: &str) -> String {
    format!(
        "DELETE FROM \"{}\" WHERE \"{}\" = '{}'",
        table_name,
        pk_column,
        escape_sql_string(pk_value)
    )
}

/// Convert JSON value to SQL literal
fn json_to_sql_literal(val: &JsonValue) -> String {
    match val {
        JsonValue::Null => "NULL".to_string(),
        JsonValue::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
        JsonValue::Number(n) => n.to_string(),
        JsonValue::String(s) => format!("'{}'", escape_sql_string(s)),
        JsonValue::Array(arr) => {
            // Convert array to SQL array literal or JSON
            let items: Vec<String> = arr.iter().map(json_to_sql_literal).collect();
            format!("ARRAY[{}]", items.join(", "))
        }
        JsonValue::Object(_) => {
            // JSON object - store as JSON string
            format!("'{}'", escape_sql_string(&val.to_string()))
        }
    }
}

/// Get the primary key column name for a table using shared database
async fn get_primary_key_column(shared_db: &SharedDatabase, table_name: &str) -> Option<String> {
    let db = shared_db.read().await;
    let table = db.get_table(table_name)?;
    let pk = table.schema.primary_key.as_ref()?;
    // For now, support single-column primary keys
    pk.first().cloned()
}

/// Convert execution result rows to JSON objects
fn rows_to_json(columns: &[String], rows: &[Vec<JsonValue>]) -> Vec<HashMap<String, JsonValue>> {
    rows.iter()
        .map(|row| {
            columns.iter().zip(row.iter()).map(|(col, val)| (col.clone(), val.clone())).collect()
        })
        .collect()
}

// ============================================================================
// CRUD Endpoints
// ============================================================================

/// GET /api/tables/:table/rows - List all rows with filtering, sorting, pagination
pub async fn list_rows(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path(table_name): Path<String>,
    Query(params): Query<CrudQueryParams>,
) -> impl IntoResponse {
    debug!("CRUD: GET /api/tables/{}/rows with params: {:?}", table_name, params);

    // Get the database name from headers
    let db_name = get_database_name(&headers);

    // Get or create the shared database from the registry
    let shared_db = state.registry.get_or_create(&db_name).await;

    // Check if table exists
    {
        let db = shared_db.read().await;
        let table_names = db.list_tables();
        if !table_names.iter().any(|t| t.eq_ignore_ascii_case(&table_name)) {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({ "error": format!("Table '{}' not found", table_name) })),
            )
                .into_response();
        }
    }

    // Build and execute SQL
    let sql = build_select_sql(&table_name, &params);
    debug!("CRUD: Executing SQL: {}", sql);

    let mut session =
        crate::session::Session::new(db_name.clone(), "http_user".to_string(), shared_db);

    match session.execute(&sql).await {
        Ok(crate::session::ExecutionResult::Select { rows, columns }) => {
            let column_names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
            let row_values: Vec<Vec<JsonValue>> =
                rows.iter().map(|r| r.values.iter().map(sql_value_to_json).collect()).collect();

            let data = rows_to_json(&column_names, &row_values);

            (StatusCode::OK, Json(CollectionResponse { data, total: None })).into_response()
        }
        Ok(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::new("Unexpected query result type")),
        )
            .into_response(),
        Err(e) => {
            error!("Query execution failed: {}", e);
            (StatusCode::BAD_REQUEST, Json(ErrorResponse::new(format!("Query failed: {}", e))))
                .into_response()
        }
    }
}

/// GET /api/tables/:table/rows/:id - Get a single row by primary key
pub async fn get_row(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path((table_name, id)): Path<(String, String)>,
    Query(params): Query<CrudQueryParams>,
) -> impl IntoResponse {
    debug!("CRUD: GET /api/tables/{}/rows/{}", table_name, id);

    // Get the database name from headers
    let db_name = get_database_name(&headers);

    // Get or create the shared database from the registry
    let shared_db = state.registry.get_or_create(&db_name).await;

    // Get primary key column
    let pk_column = match get_primary_key_column(&shared_db, &table_name).await {
        Some(pk) => pk,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::new(format!(
                    "Table '{}' has no primary key defined. Cannot use resource-by-id endpoint.",
                    table_name
                ))),
            )
                .into_response();
        }
    };

    // Build and execute SQL
    let sql = build_select_by_pk_sql(&table_name, &pk_column, &id, params.select.as_deref());
    debug!("CRUD: Executing SQL: {}", sql);

    let mut session =
        crate::session::Session::new(db_name.clone(), "http_user".to_string(), shared_db);

    match session.execute(&sql).await {
        Ok(crate::session::ExecutionResult::Select { rows, columns }) => {
            if rows.is_empty() {
                return (
                    StatusCode::NOT_FOUND,
                    Json(ErrorResponse::new(format!("Resource with id '{}' not found", id))),
                )
                    .into_response();
            }

            let column_names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
            let row_values: Vec<JsonValue> = rows[0].values.iter().map(sql_value_to_json).collect();

            let data: HashMap<String, JsonValue> =
                column_names.into_iter().zip(row_values).collect();

            (StatusCode::OK, Json(ResourceResponse { data })).into_response()
        }
        Ok(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::new("Unexpected query result type")),
        )
            .into_response(),
        Err(e) => {
            error!("Query execution failed: {}", e);
            (StatusCode::BAD_REQUEST, Json(ErrorResponse::new(format!("Query failed: {}", e))))
                .into_response()
        }
    }
}

/// POST /api/tables/:table/rows - Create a new row
pub async fn create_row(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path(table_name): Path<String>,
    Json(body): Json<CreateRequest>,
) -> impl IntoResponse {
    debug!("CRUD: POST /api/tables/{}/rows with body: {:?}", table_name, body);

    if body.values.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::new("Request body must contain column values")),
        )
            .into_response();
    }

    // Get the database name from headers
    let db_name = get_database_name(&headers);

    // Get or create the shared database from the registry
    let shared_db = state.registry.get_or_create(&db_name).await;

    // Check if table exists
    {
        let db = shared_db.read().await;
        let table_names = db.list_tables();
        if !table_names.iter().any(|t| t.eq_ignore_ascii_case(&table_name)) {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({ "error": format!("Table '{}' not found", table_name) })),
            )
                .into_response();
        }
    }

    // Build and execute SQL
    let sql = build_insert_sql(&table_name, &body.values);
    debug!("CRUD: Executing SQL: {}", sql);

    let mut session =
        crate::session::Session::new(db_name.clone(), "http_user".to_string(), shared_db);

    match session.execute(&sql).await {
        Ok(crate::session::ExecutionResult::Insert { rows_affected }) => {
            (StatusCode::CREATED, Json(MutationResponse { rows_affected })).into_response()
        }
        Ok(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::new("Unexpected query result type")),
        )
            .into_response(),
        Err(e) => {
            error!("Insert failed: {}", e);
            (StatusCode::BAD_REQUEST, Json(ErrorResponse::new(format!("Insert failed: {}", e))))
                .into_response()
        }
    }
}

/// PUT /api/tables/:table/rows/:id - Full update (replace all columns)
pub async fn update_row(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path((table_name, id)): Path<(String, String)>,
    Json(body): Json<UpdateRequest>,
) -> impl IntoResponse {
    debug!("CRUD: PUT /api/tables/{}/rows/{} with body: {:?}", table_name, id, body);

    if body.values.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::new("Request body must contain column values")),
        )
            .into_response();
    }

    // Get the database name from headers
    let db_name = get_database_name(&headers);

    // Get or create the shared database from the registry
    let shared_db = state.registry.get_or_create(&db_name).await;

    // Get primary key column
    let pk_column = match get_primary_key_column(&shared_db, &table_name).await {
        Some(pk) => pk,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::new(format!(
                    "Table '{}' has no primary key defined. Cannot use resource-by-id endpoint.",
                    table_name
                ))),
            )
                .into_response();
        }
    };

    // Build and execute SQL
    let sql = build_update_sql(&table_name, &pk_column, &id, &body.values);
    debug!("CRUD: Executing SQL: {}", sql);

    let mut session =
        crate::session::Session::new(db_name.clone(), "http_user".to_string(), shared_db);

    match session.execute(&sql).await {
        Ok(crate::session::ExecutionResult::Update { rows_affected }) => {
            if rows_affected == 0 {
                (
                    StatusCode::NOT_FOUND,
                    Json(ErrorResponse::new(format!("Resource with id '{}' not found", id))),
                )
                    .into_response()
            } else {
                (StatusCode::OK, Json(MutationResponse { rows_affected })).into_response()
            }
        }
        Ok(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::new("Unexpected query result type")),
        )
            .into_response(),
        Err(e) => {
            error!("Update failed: {}", e);
            (StatusCode::BAD_REQUEST, Json(ErrorResponse::new(format!("Update failed: {}", e))))
                .into_response()
        }
    }
}

/// PATCH /api/tables/:table/rows/:id - Partial update (only specified columns)
pub async fn patch_row(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path((table_name, id)): Path<(String, String)>,
    Json(body): Json<PatchRequest>,
) -> impl IntoResponse {
    debug!("CRUD: PATCH /api/tables/{}/rows/{} with body: {:?}", table_name, id, body);

    if body.values.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::new("Request body must contain at least one column to update")),
        )
            .into_response();
    }

    // Get the database name from headers
    let db_name = get_database_name(&headers);

    // Get or create the shared database from the registry
    let shared_db = state.registry.get_or_create(&db_name).await;

    // Get primary key column
    let pk_column = match get_primary_key_column(&shared_db, &table_name).await {
        Some(pk) => pk,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::new(format!(
                    "Table '{}' has no primary key defined. Cannot use resource-by-id endpoint.",
                    table_name
                ))),
            )
                .into_response();
        }
    };

    // Build and execute SQL (same as PUT, but caller decides which columns to include)
    let sql = build_update_sql(&table_name, &pk_column, &id, &body.values);
    debug!("CRUD: Executing SQL: {}", sql);

    let mut session =
        crate::session::Session::new(db_name.clone(), "http_user".to_string(), shared_db);

    match session.execute(&sql).await {
        Ok(crate::session::ExecutionResult::Update { rows_affected }) => {
            if rows_affected == 0 {
                (
                    StatusCode::NOT_FOUND,
                    Json(ErrorResponse::new(format!("Resource with id '{}' not found", id))),
                )
                    .into_response()
            } else {
                (StatusCode::OK, Json(MutationResponse { rows_affected })).into_response()
            }
        }
        Ok(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::new("Unexpected query result type")),
        )
            .into_response(),
        Err(e) => {
            error!("Patch failed: {}", e);
            (StatusCode::BAD_REQUEST, Json(ErrorResponse::new(format!("Patch failed: {}", e))))
                .into_response()
        }
    }
}

/// DELETE /api/tables/:table/rows/:id - Delete a row
pub async fn delete_row(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path((table_name, id)): Path<(String, String)>,
) -> impl IntoResponse {
    debug!("CRUD: DELETE /api/tables/{}/rows/{}", table_name, id);

    // Get the database name from headers
    let db_name = get_database_name(&headers);

    // Get or create the shared database from the registry
    let shared_db = state.registry.get_or_create(&db_name).await;

    // Get primary key column
    let pk_column = match get_primary_key_column(&shared_db, &table_name).await {
        Some(pk) => pk,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::new(format!(
                    "Table '{}' has no primary key defined. Cannot use resource-by-id endpoint.",
                    table_name
                ))),
            )
                .into_response();
        }
    };

    // Build and execute SQL
    let sql = build_delete_sql(&table_name, &pk_column, &id);
    debug!("CRUD: Executing SQL: {}", sql);

    let mut session =
        crate::session::Session::new(db_name.clone(), "http_user".to_string(), shared_db);

    match session.execute(&sql).await {
        Ok(crate::session::ExecutionResult::Delete { rows_affected }) => {
            if rows_affected == 0 {
                (
                    StatusCode::NOT_FOUND,
                    Json(ErrorResponse::new(format!("Resource with id '{}' not found", id))),
                )
                    .into_response()
            } else {
                (StatusCode::OK, Json(MutationResponse { rows_affected })).into_response()
            }
        }
        Ok(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::new("Unexpected query result type")),
        )
            .into_response(),
        Err(e) => {
            error!("Delete failed: {}", e);
            (StatusCode::BAD_REQUEST, Json(ErrorResponse::new(format!("Delete failed: {}", e))))
                .into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filter_operator_parse() {
        assert!(
            matches!(FilterOperator::parse("eq.hello"), Ok(FilterOperator::Eq(s)) if s == "hello")
        );
        assert!(
            matches!(FilterOperator::parse("neq.world"), Ok(FilterOperator::Neq(s)) if s == "world")
        );
        assert!(matches!(FilterOperator::parse("gt.10"), Ok(FilterOperator::Gt(s)) if s == "10"));
        assert!(matches!(FilterOperator::parse("gte.5"), Ok(FilterOperator::Gte(s)) if s == "5"));
        assert!(matches!(FilterOperator::parse("lt.100"), Ok(FilterOperator::Lt(s)) if s == "100"));
        assert!(matches!(FilterOperator::parse("lte.50"), Ok(FilterOperator::Lte(s)) if s == "50"));
        assert!(
            matches!(FilterOperator::parse("like.%test%"), Ok(FilterOperator::Like(s)) if s == "%test%")
        );
        assert!(
            matches!(FilterOperator::parse("ilike.%TEST%"), Ok(FilterOperator::Ilike(s)) if s == "%TEST%")
        );
        assert!(matches!(FilterOperator::parse("is.null"), Ok(FilterOperator::Is(IsValue::Null))));
        assert!(matches!(
            FilterOperator::parse("is.notnull"),
            Ok(FilterOperator::Is(IsValue::NotNull))
        ));

        // IN operator
        if let Ok(FilterOperator::In(items)) = FilterOperator::parse("in.(a,b,c)") {
            assert_eq!(items, vec!["a", "b", "c"]);
        } else {
            panic!("Expected In operator");
        }

        // Plain value (no operator) defaults to eq
        assert!(
            matches!(FilterOperator::parse("plain_value"), Ok(FilterOperator::Eq(s)) if s == "plain_value")
        );
    }

    #[test]
    fn test_filter_operator_to_sql() {
        assert_eq!(
            FilterOperator::Eq("test".to_string()).to_sql_condition("name"),
            "\"name\" = 'test'"
        );
        assert_eq!(
            FilterOperator::Neq("test".to_string()).to_sql_condition("name"),
            "\"name\" <> 'test'"
        );
        assert_eq!(FilterOperator::Gt("10".to_string()).to_sql_condition("age"), "\"age\" > '10'");
        assert_eq!(
            FilterOperator::In(vec!["a".to_string(), "b".to_string()]).to_sql_condition("status"),
            "\"status\" IN ('a', 'b')"
        );
        assert_eq!(
            FilterOperator::Is(IsValue::Null).to_sql_condition("deleted_at"),
            "\"deleted_at\" IS NULL"
        );
    }

    #[test]
    fn test_build_select_sql() {
        let mut params = CrudQueryParams::default();
        assert_eq!(build_select_sql("users", &params), "SELECT * FROM \"users\"");

        params.select = Some("id,name".to_string());
        assert_eq!(build_select_sql("users", &params), "SELECT \"id\", \"name\" FROM \"users\"");

        params.order = Some("created_at.desc".to_string());
        assert_eq!(
            build_select_sql("users", &params),
            "SELECT \"id\", \"name\" FROM \"users\" ORDER BY \"created_at\" DESC"
        );

        params.limit = Some(10);
        params.offset = Some(20);
        assert_eq!(
            build_select_sql("users", &params),
            "SELECT \"id\", \"name\" FROM \"users\" ORDER BY \"created_at\" DESC LIMIT 10 OFFSET 20"
        );
    }

    #[test]
    fn test_build_select_with_filters() {
        let mut params = CrudQueryParams::default();
        params.filters.insert("age".to_string(), "gt.21".to_string());
        params.filters.insert("status".to_string(), "eq.active".to_string());

        let sql = build_select_sql("users", &params);
        // Order of filters may vary due to HashMap
        assert!(sql.contains("\"age\" > '21'"));
        assert!(sql.contains("\"status\" = 'active'"));
        assert!(sql.contains(" AND "));
    }

    #[test]
    fn test_escape_sql_string() {
        assert_eq!(escape_sql_string("hello"), "hello");
        assert_eq!(escape_sql_string("it's"), "it''s");
        assert_eq!(escape_sql_string("test'value'here"), "test''value''here");
    }

    #[test]
    fn test_json_to_sql_literal() {
        assert_eq!(json_to_sql_literal(&JsonValue::Null), "NULL");
        assert_eq!(json_to_sql_literal(&JsonValue::Bool(true)), "TRUE");
        assert_eq!(json_to_sql_literal(&JsonValue::Bool(false)), "FALSE");
        assert_eq!(json_to_sql_literal(&json!(42)), "42");
        assert_eq!(json_to_sql_literal(&json!(3.25)), "3.25");
        assert_eq!(json_to_sql_literal(&json!("hello")), "'hello'");
        assert_eq!(json_to_sql_literal(&json!("it's")), "'it''s'");
    }
}
