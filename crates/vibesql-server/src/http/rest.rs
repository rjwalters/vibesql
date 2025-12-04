//! REST API endpoints for VibeSQL HTTP interface

use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
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
        .route("/api/subscribe", get(subscribe_stream))
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

// ============================================================================
// SSE Subscription Endpoint
// ============================================================================

/// Query parameters for subscription endpoint
#[derive(Debug, Deserialize)]
pub struct SubscribeQuery {
    /// SQL query to subscribe to
    pub query: String,
    /// Optional query parameters (comma-separated values)
    #[serde(default)]
    pub params: Option<String>,
}

/// SSE event sent to clients
#[derive(Debug, serde::Serialize)]
pub struct SseEvent {
    /// Event type: "initial", "insert", "update", "delete", "error"
    #[serde(rename = "type")]
    pub event_type: String,
    /// Column names (sent with initial event)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub columns: Option<Vec<String>>,
    /// All rows in result set (for initial and full updates)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows: Option<Vec<Vec<serde_json::Value>>>,
    /// Old row value (for updates)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub old: Option<Vec<serde_json::Value>>,
    /// New row value (for updates and inserts)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new: Option<Vec<serde_json::Value>>,
    /// Error message
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Server-Sent Events subscription endpoint
/// 
/// GET /api/subscribe?query=SELECT%20*%20FROM%20users
/// 
/// Returns a text/event-stream response with real-time updates
async fn subscribe_stream(
    State(_state): State<HttpState>,
    Query(params): Query<SubscribeQuery>,
) -> axum::response::Response {
    use axum::response::sse::{Event, KeepAlive, Sse};

    debug!("SSE subscription requested for query: {}", params.query);

    // Parse optional parameters
    let params_vec = if let Some(params_str) = params.params {
        let mut values = Vec::new();
        for s in params_str.split(',') {
            use vibesql_types::SqlValue;
            let val = if let Ok(i) = s.trim().parse::<i64>() {
                SqlValue::Integer(i)
            } else {
                SqlValue::Varchar(s.trim().to_string())
            };
            values.push(val);
        }
        values
    } else {
        vec![]
    };

    // Execute initial query
    let mut session = match crate::session::Session::new("http".to_string(), "http_user".to_string()) {
        Ok(s) => s,
        Err(e) => {
            error!("Failed to create session: {}", e);
            let event_data = serde_json::to_string(&SseEvent {
                event_type: "error".to_string(),
                columns: None,
                rows: None,
                old: None,
                new: None,
                error: Some(format!("Failed to create session: {}", e)),
            }).unwrap_or_default();
            
            let stream = futures::stream::once(async move {
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(
                    Event::default().data(event_data)
                )
            });
            
            return Sse::new(stream)
                .keep_alive(KeepAlive::default())
                .into_response();
        }
    };

    // Execute the initial query
    let result = if params_vec.is_empty() {
        session.execute(&params.query)
    } else {
        session.execute_with_params(&params.query, &params_vec)
    };

    let (columns, rows) = match result {
        Ok(crate::session::ExecutionResult::Select { rows, columns }) => {
            let column_names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
            let row_values: Vec<Vec<_>> = rows
                .iter()
                .map(|r| r.values.iter().map(super::types::sql_value_to_json).collect())
                .collect();
            (column_names, row_values)
        }
        Ok(_) => {
            error!("Subscription query must be a SELECT statement");
            let event_data = serde_json::to_string(&SseEvent {
                event_type: "error".to_string(),
                columns: None,
                rows: None,
                old: None,
                new: None,
                error: Some("Subscription query must be a SELECT statement".to_string()),
            }).unwrap_or_default();
            
            let stream = futures::stream::once(async move {
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(
                    Event::default().data(event_data)
                )
            });
            
            return Sse::new(stream)
                .keep_alive(KeepAlive::default())
                .into_response();
        }
        Err(e) => {
            error!("Query execution failed: {}", e);
            let event_data = serde_json::to_string(&SseEvent {
                event_type: "error".to_string(),
                columns: None,
                rows: None,
                old: None,
                new: None,
                error: Some(format!("Query execution failed: {}", e)),
            }).unwrap_or_default();
            
            let stream = futures::stream::once(async move {
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(
                    Event::default().data(event_data)
                )
            });
            
            return Sse::new(stream)
                .keep_alive(KeepAlive::default())
                .into_response();
        }
    };

    // Send initial result set and keepalive messages
    let initial_event_data = serde_json::to_string(&SseEvent {
        event_type: "initial".to_string(),
        columns: Some(columns),
        rows: Some(rows),
        old: None,
        new: None,
        error: None,
    }).unwrap_or_default();

    // Create stream that sends initial result then keepalives
    let stream = {
        let initial = Event::default().data(initial_event_data);
        let mut events = vec![Ok::<_, Box<dyn std::error::Error + Send + Sync>>(initial)];
        
        // For now, add a placeholder keepalive. Real implementation would subscribe
        // to changes and stream updates continuously
        events.push(Ok(Event::default().comment("TODO: add real-time updates")));
        
        futures::stream::iter(events)
    };

    // Create SSE response with keepalive
    Sse::new(stream)
        .keep_alive(KeepAlive::default())
        .into_response()
}
