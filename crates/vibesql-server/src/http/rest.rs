//! REST API endpoints for VibeSQL HTTP interface

use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, patch, post, put},
    Json, Router,
};
use serde::Deserialize;
use serde_json::json;
use tracing::{debug, error};

use vibesql_storage::Database;

use super::types::*;
use super::graphql;

/// Pagination configuration
#[derive(Debug, Clone)]
pub struct PaginationParams {
    /// Number of rows to skip
    pub offset: usize,
    /// Maximum rows to return
    pub limit: usize,
}

impl PaginationParams {
    /// Create pagination from request parameters
    pub fn from_request(limit: Option<usize>, offset: Option<usize>) -> Self {
        Self {
            offset: offset.unwrap_or(0),
            limit: limit.unwrap_or(usize::MAX),
        }
    }

    /// Apply pagination to results
    pub fn apply(&self, rows: Vec<Vec<serde_json::Value>>) -> (Vec<Vec<serde_json::Value>>, usize) {
        let total_count = rows.len();
        let paginated = rows
            .into_iter()
            .skip(self.offset)
            .take(self.limit)
            .collect();
        (paginated, total_count)
    }
}

/// HTTP server state
#[derive(Clone)]
pub struct HttpState {
    pub db: Arc<Database>,
}

/// Create the HTTP API router
pub fn create_http_router(db: Arc<Database>) -> Router {
    let state = HttpState { db: db.clone() };

    // Create main router with state
    let main_router = Router::new()
        .route("/health", get(health_check))
        .route("/api/query", post(execute_query))
        .route("/api/subscribe", get(subscribe_stream))
        .route("/api/tables", get(list_tables))
        .route("/api/tables/:table_name", get(get_table_info))
        // CRUD endpoints for auto-generated RESTful access
        .route("/api/tables/:table_name/rows", get(super::crud::list_rows))
        .route("/api/tables/:table_name/rows", post(super::crud::create_row))
        .route("/api/tables/:table_name/rows/:id", get(super::crud::get_row))
        .route("/api/tables/:table_name/rows/:id", put(super::crud::update_row))
        .route("/api/tables/:table_name/rows/:id", patch(super::crud::patch_row))
        .route("/api/tables/:table_name/rows/:id", delete(super::crud::delete_row))
        // GraphQL endpoint
        .route("/api/graphql", post(graphql_handler))
        .with_state(state);

    // Create storage sub-router with its own state
    // We nest it after the main router is state-resolved
    let storage_router = super::storage::create_storage_router(db);

    main_router.nest("/api/storage", storage_router)
}

/// GraphQL endpoint handler
async fn graphql_handler(
    State(state): State<HttpState>,
    Json(req): Json<graphql::GraphQLRequest>,
) -> impl IntoResponse {
    debug!("Received GraphQL request: {}", req.query);

    // First, try to handle introspection queries
    if let Some(introspection_result) = graphql::try_introspection_query(&state.db, &req.query) {
        return (
            StatusCode::OK,
            Json(graphql::GraphQLResponse {
                data: Some(introspection_result),
                errors: None,
            }),
        )
            .into_response();
    }

    // Parse the GraphQL query
    let query_info = match graphql::parse_graphql_query(&req.query) {
        Ok(info) => info,
        Err(e) => {
            error!("Failed to parse GraphQL query: {}", e);
            return (
                StatusCode::BAD_REQUEST,
                Json(graphql::GraphQLResponse {
                    data: None,
                    errors: Some(vec![graphql::GraphQLError::new(format!(
                        "GraphQL parse error: {}",
                        e
                    ))]),
                }),
            )
                .into_response();
        }
    };

    // Convert to SQL
    let (sql, params) = match graphql::graphql_to_sql(&query_info) {
        Ok((sql, params)) => (sql, params),
        Err(e) => {
            error!("Failed to convert GraphQL to SQL: {}", e);
            return (
                StatusCode::BAD_REQUEST,
                Json(graphql::GraphQLResponse {
                    data: None,
                    errors: Some(vec![graphql::GraphQLError::new(format!(
                        "GraphQL conversion error: {}",
                        e
                    ))]),
                }),
            )
                .into_response();
        }
    };

    debug!("Generated SQL: {}", sql);

    // Create a session and execute the query
    let mut session = match crate::session::Session::new("graphql".to_string(), "graphql_user".to_string()) {
        Ok(s) => s,
        Err(e) => {
            error!("Failed to create session: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(graphql::GraphQLResponse {
                    data: None,
                    errors: Some(vec![graphql::GraphQLError::new(format!(
                        "Failed to create session: {}",
                        e
                    ))]),
                }),
            )
                .into_response();
        }
    };

    // Execute the query
    let result = if params.is_empty() {
        session.execute(&sql)
    } else {
        session.execute_with_params(&sql, &params)
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

                    let rows_json: Vec<serde_json::Value> = row_values
                        .iter()
                        .map(|row| {
                            let mut obj = serde_json::Map::new();
                            for (col, val) in column_names.iter().zip(row.iter()) {
                                obj.insert(col.clone(), val.clone());
                            }
                            serde_json::Value::Object(obj)
                        })
                        .collect();

                    let response = graphql::GraphQLResponse {
                        data: Some(json!({
                            "data": rows_json
                        })),
                        errors: None,
                    };

                    (StatusCode::OK, Json(response)).into_response()
                }
                crate::session::ExecutionResult::Insert { rows_affected } => {
                    let response = graphql::GraphQLResponse {
                        data: Some(json!({
                            "rowsAffected": rows_affected
                        })),
                        errors: None,
                    };

                    (StatusCode::OK, Json(response)).into_response()
                }
                crate::session::ExecutionResult::Update { rows_affected } => {
                    let response = graphql::GraphQLResponse {
                        data: Some(json!({
                            "rowsAffected": rows_affected
                        })),
                        errors: None,
                    };

                    (StatusCode::OK, Json(response)).into_response()
                }
                crate::session::ExecutionResult::Delete { rows_affected } => {
                    let response = graphql::GraphQLResponse {
                        data: Some(json!({
                            "rowsAffected": rows_affected
                        })),
                        errors: None,
                    };

                    (StatusCode::OK, Json(response)).into_response()
                }
                _ => {
                    let response = graphql::GraphQLResponse {
                        data: Some(json!({
                            "status": "success",
                            "message": format!("{:?}", exec_result)
                        })),
                        errors: None,
                    };

                    (StatusCode::OK, Json(response)).into_response()
                }
            }
        }
        Err(e) => {
            error!("Query execution failed: {}", e);
            (
                StatusCode::BAD_REQUEST,
                Json(graphql::GraphQLResponse {
                    data: None,
                    errors: Some(vec![graphql::GraphQLError::new(format!(
                        "Query execution failed: {}",
                        e
                    ))]),
                }),
            )
                .into_response()
        }
    }
}

/// Health check endpoint
async fn health_check() -> impl IntoResponse {
    Json(HealthResponse {
        status: "ok".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
    })
}

/// Execute a SQL query with optional pagination
async fn execute_query(
    State(_state): State<HttpState>,
    Json(req): Json<QueryRequest>,
) -> impl IntoResponse {
    debug!("Executing query: {} (limit: {:?}, offset: {:?})", req.sql, req.limit, req.offset);

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

                    // Apply pagination
                    let pagination = PaginationParams::from_request(req.limit, req.offset);
                    let (paginated_rows, total_count) = pagination.apply(row_values);

                    let response = QueryResponse {
                        columns: column_names,
                        row_count: paginated_rows.len(),
                        rows: paginated_rows,
                        total_count: Some(total_count),
                        offset: req.offset,
                        limit: req.limit,
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
    // Try to get the table (with case-insensitive lookup)
    let table = state.db.get_table(&table_name);

    if table.is_none() {
        // Try case-insensitive lookup
        let table_names = state.db.list_tables();
        if !table_names.iter().any(|t| t.eq_ignore_ascii_case(&table_name)) {
            return (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse::new(format!("Table '{}' not found", table_name))),
            )
                .into_response();
        }
    }

    // Get schema information
    if let Some(table) = state.db.get_table(&table_name) {
        let schema = &table.schema;
        let pk_columns: Vec<&String> = schema.primary_key.as_ref().map(|pk| pk.iter().collect()).unwrap_or_default();

        let columns: Vec<ColumnInfo> = schema
            .columns
            .iter()
            .map(|col| ColumnInfo {
                name: col.name.clone(),
                data_type: format!("{:?}", col.data_type),
                nullable: col.nullable,
                primary_key: pk_columns.contains(&&col.name),
            })
            .collect();

        let info = TableInfo { name: table_name, columns };
        return (StatusCode::OK, Json(info)).into_response();
    }

    // Fallback: return minimal info if we couldn't get schema
    let columns = vec![ColumnInfo {
        name: "*".to_string(),
        data_type: "unknown".to_string(),
        nullable: true,
        primary_key: false,
    }];

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pagination_from_request_defaults() {
        let pagination = PaginationParams::from_request(None, None);
        assert_eq!(pagination.offset, 0);
        assert_eq!(pagination.limit, usize::MAX);
    }

    #[test]
    fn test_pagination_from_request_with_limit() {
        let pagination = PaginationParams::from_request(Some(10), None);
        assert_eq!(pagination.offset, 0);
        assert_eq!(pagination.limit, 10);
    }

    #[test]
    fn test_pagination_from_request_with_offset() {
        let pagination = PaginationParams::from_request(None, Some(5));
        assert_eq!(pagination.offset, 5);
        assert_eq!(pagination.limit, usize::MAX);
    }

    #[test]
    fn test_pagination_from_request_with_both() {
        let pagination = PaginationParams::from_request(Some(10), Some(5));
        assert_eq!(pagination.offset, 5);
        assert_eq!(pagination.limit, 10);
    }

    #[test]
    fn test_pagination_apply_basic() {
        let pagination = PaginationParams::from_request(Some(2), Some(1));
        let rows = vec![
            vec![serde_json::json!("a")],
            vec![serde_json::json!("b")],
            vec![serde_json::json!("c")],
            vec![serde_json::json!("d")],
        ];

        let (paginated, total) = pagination.apply(rows);
        assert_eq!(total, 4, "Total should be 4");
        assert_eq!(paginated.len(), 2, "Paginated should have 2 rows");
    }

    #[test]
    fn test_pagination_apply_offset_exceeds_total() {
        let pagination = PaginationParams::from_request(Some(10), Some(100));
        let rows = vec![
            vec![serde_json::json!("a")],
            vec![serde_json::json!("b")],
        ];

        let (paginated, total) = pagination.apply(rows);
        assert_eq!(total, 2, "Total should be 2");
        assert_eq!(paginated.len(), 0, "Paginated should be empty");
    }

    #[test]
    fn test_pagination_apply_no_limit() {
        let pagination = PaginationParams::from_request(None, Some(1));
        let rows = vec![
            vec![serde_json::json!("a")],
            vec![serde_json::json!("b")],
            vec![serde_json::json!("c")],
        ];

        let (paginated, total) = pagination.apply(rows);
        assert_eq!(total, 3, "Total should be 3");
        assert_eq!(paginated.len(), 2, "Should return remaining rows");
    }

    #[test]
    fn test_pagination_apply_empty_rows() {
        let pagination = PaginationParams::from_request(Some(10), Some(5));
        let rows: Vec<Vec<serde_json::Value>> = vec![];

        let (paginated, total) = pagination.apply(rows);
        assert_eq!(total, 0, "Total should be 0");
        assert_eq!(paginated.len(), 0, "Paginated should be empty");
    }
}
