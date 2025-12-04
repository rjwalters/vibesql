//! REST API endpoints for VibeSQL HTTP interface

use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde_json::json;
use tracing::{debug, error};

use vibesql_storage::Database;

use super::types::*;

/// HTTP server state
#[derive(Clone)]
pub struct HttpState {
    pub db: Arc<Database>,
}

/// Create the HTTP API router
pub fn create_http_router(db: Arc<Database>) -> Router {
    let state = HttpState { db };

    Router::new()
        .route("/health", get(health_check))
        .route("/api/query", post(execute_query))
        .route("/api/tables", get(list_tables))
        .route("/api/tables/:table_name", get(get_table_info))
        .with_state(state)
}

/// Health check endpoint
async fn health_check() -> impl IntoResponse {
    Json(HealthResponse {
        status: "ok".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
    })
}

/// Execute a SQL query
async fn execute_query(
    State(_state): State<HttpState>,
    Json(req): Json<QueryRequest>,
) -> impl IntoResponse {
    debug!("Executing query: {}", req.sql);

    // Convert JSON parameters to SqlValue
    let params = match req.to_sql_values() {
        Ok(p) => p,
        Err(e) => {
            error!("Failed to convert parameters: {}", e);
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::new(format!("Invalid parameters: {}", e))),
            )
                .into_response();
        }
    };

    // Create a session for query execution
    let mut session = match crate::session::Session::new("http".to_string(), "http_user".to_string()) {
        Ok(s) => s,
        Err(e) => {
            error!("Failed to create session: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::new(format!("Failed to create session: {}", e))),
            )
                .into_response();
        }
    };

    // Execute the query
    let result = if params.is_empty() {
        session.execute(&req.sql)
    } else {
        session.execute_with_params(&req.sql, &params)
    };

    match result {
        Ok(exec_result) => {
            match exec_result {
                crate::session::ExecutionResult::Select { rows, columns } => {
                    let column_names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
                    let row_values: Vec<Vec<_>> = rows
                        .iter()
                        .map(|r| r.values.iter().map(super::types::sql_value_to_json).collect())
                        .collect();

                    let response = QueryResponse {
                        columns: column_names,
                        row_count: row_values.len(),
                        rows: row_values,
                    };

                    (StatusCode::OK, Json(response)).into_response()
                }
                crate::session::ExecutionResult::Insert { rows_affected } => {
                    let response = MutationResponse { rows_affected };
                    (StatusCode::CREATED, Json(response)).into_response()
                }
                crate::session::ExecutionResult::Update { rows_affected } => {
                    let response = MutationResponse { rows_affected };
                    (StatusCode::OK, Json(response)).into_response()
                }
                crate::session::ExecutionResult::Delete { rows_affected } => {
                    let response = MutationResponse { rows_affected };
                    (StatusCode::OK, Json(response)).into_response()
                }
                _ => {
                    let response = json!({
                        "status": "success",
                        "message": format!("{:?}", exec_result)
                    });
                    (StatusCode::OK, Json(response)).into_response()
                }
            }
        }
        Err(e) => {
            error!("Query execution failed: {}", e);
            (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::new(format!("Query execution failed: {}", e))),
            )
                .into_response()
        }
    }
}

/// List all tables in the database
async fn list_tables(State(state): State<HttpState>) -> impl IntoResponse {
    let table_names = state.db.list_tables();

    Json(json!({
        "tables": table_names,
        "count": table_names.len()
    }))
}

/// Get information about a specific table
async fn get_table_info(
    State(state): State<HttpState>,
    Path(table_name): Path<String>,
) -> impl IntoResponse {
    let table_names = state.db.list_tables();

    if !table_names.contains(&table_name) {
        return (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse::new(format!("Table '{}' not found", table_name))),
        )
            .into_response();
    }

    // For now, we return a minimal table info
    // In the future, we can enhance this to get actual column information
    let columns = vec![
        ColumnInfo {
            name: "*".to_string(),
            data_type: "unknown".to_string(),
            nullable: true,
            primary_key: false,
        }
    ];

    let info = TableInfo { name: table_name, columns };

    (StatusCode::OK, Json(info)).into_response()
}
