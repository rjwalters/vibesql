//! REST API endpoints for VibeSQL HTTP interface

use std::sync::{Arc, Arc as StdArc};

use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{delete, get, patch, post, put},
    Json, Router,
};
use serde::Deserialize;
use serde_json::json;
use tokio::sync::mpsc;
use tracing::{debug, error};
use vibesql_storage::Database;

use super::{graphql, types::*};
use crate::{
    observability::ServerMetrics,
    registry::DatabaseRegistry,
    replication::{
        role_str, ReplicationHandle, SqlError, SQLSTATE_FATAL, SQLSTATE_NOT_LEADER, SQLSTATE_RETRY,
    },
    session::Session,
    subscription::{
        detect_pk_columns_from_stmt, SelectiveColumnConfig, SubscriptionManager, SubscriptionUpdate,
    },
};

/// Response header carrying the leader hint on a `421 Misdirected Request`
/// (the HTTP equivalent of the wire path's NOT_LEADER redirect, #5410).
pub const LEADER_HINT_HEADER: &str = "X-VibeSQL-Leader";

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
        Self { offset: offset.unwrap_or(0), limit: limit.unwrap_or(usize::MAX) }
    }

    /// Apply pagination to results
    pub fn apply(&self, rows: Vec<Vec<serde_json::Value>>) -> (Vec<Vec<serde_json::Value>>, usize) {
        let total_count = rows.len();
        let paginated = rows.into_iter().skip(self.offset).take(self.limit).collect();
        (paginated, total_count)
    }
}

/// Default database name for HTTP API requests
pub const DEFAULT_DATABASE_NAME: &str = "default";

/// HTTP header for specifying the database name
pub const DATABASE_HEADER: &str = "X-Database-Name";

/// HTTP server state
#[derive(Clone)]
pub struct HttpState {
    /// Database registry for shared database access
    pub registry: DatabaseRegistry,
    /// Legacy database reference for backwards compatibility (e.g., subscriptions, table listing)
    pub db: Arc<Database>,
    /// Subscription manager for real-time updates
    pub subscription_manager: Arc<SubscriptionManager>,
    /// Optional server metrics for observability
    pub metrics: Option<ServerMetrics>,
    /// Consensus handle when the server runs in replicated mode (#5393).
    ///
    /// Present only in replicated mode. Surfaces that have been wired through
    /// consensus (#5410 — the `/api/query` endpoint and the CRUD collection
    /// endpoints; #5420 — the CRUD by-id endpoints, which resolve the primary
    /// key from the replicated catalog via
    /// [`ReplicationHandle::primary_key_column`]; #5421 — the GraphQL endpoint,
    /// which introspects the replicated catalog via
    /// [`ReplicationHandle::schema_snapshot`]) build a **replicated** session
    /// via [`Self::session`], so writes propose through this handle (leader-only,
    /// freeze-at-propose) and reads run against the replicated state machine —
    /// never against the unreplicated local registry database. Surfaces that
    /// still depend on local-database change-feed/state introspection
    /// (subscriptions, blob storage) remain gated (see [`Self::replicated_gate`])
    /// to a clear error rather than silently reading stale data or accepting a
    /// write that never replicates; those are tracked as follow-ons to #5410.
    /// `/health` uses the handle to report node role/writability.
    pub replication: Option<StdArc<ReplicationHandle>>,
}

impl HttpState {
    /// Whether this server runs in replicated mode.
    pub(crate) fn is_replicated(&self) -> bool {
        self.replication.is_some()
    }

    /// Build the session for an HTTP request against `db_name`.
    ///
    /// In replicated mode this returns a **replicated** session
    /// ([`Session::new_replicated`]): writes propose through consensus
    /// (leader-only, freeze-at-propose) and reads run against the replicated
    /// state machine per the session's read-consistency mode — the local
    /// `shared_db` is retained only for API compatibility and is never
    /// executed against. In standalone mode it returns the ordinary
    /// local-executor session, leaving standalone behavior untouched. This is
    /// the single choke point that keeps every replicated HTTP write going
    /// through consensus (the split-brain invariant, #5410).
    pub(crate) fn session(
        &self,
        db_name: &str,
        shared_db: crate::registry::SharedDatabase,
    ) -> Session {
        match &self.replication {
            Some(handle) => Session::new_replicated(
                db_name.to_string(),
                "http_user".to_string(),
                shared_db,
                StdArc::clone(handle),
            ),
            None => Session::new(db_name.to_string(), "http_user".to_string(), shared_db),
        }
    }

    /// Resolve the GraphQL schema map (table name → [`TableSchema`]) for
    /// `shared_db`, used to answer introspection queries and to drive nested
    /// relationship resolution (#5421).
    ///
    /// In replicated mode the schema lives in the consensus state machine, not
    /// the (empty) local registry database, so it is read from the **applied
    /// replicated catalog** via [`ReplicationHandle::schema_snapshot`]. In
    /// standalone mode it is built from the local registry database, exactly as
    /// the pre-#5421 path did — keeping standalone GraphQL behavior unchanged.
    pub(crate) async fn graphql_schema_map(
        &self,
        shared_db: &crate::registry::SharedDatabase,
    ) -> std::collections::HashMap<String, vibesql_catalog::TableSchema> {
        match &self.replication {
            Some(handle) => handle.schema_snapshot(),
            None => {
                let db_guard = shared_db.read().await;
                build_schema_map(&db_guard)
            }
        }
    }

    /// `Some(error_response)` when this request hits a surface that is not
    /// yet coherent in replicated mode and must be refused; `None` in
    /// standalone mode (where every path is sound). Returning a `503` with
    /// a `Retry-After`-style hint matches how a non-leader PostgreSQL
    /// session refuses writes — the caller should route to the dedicated
    /// PostgreSQL wire port, which is fully replicated.
    pub(crate) fn replicated_gate(
        &self,
        feature: &str,
    ) -> Option<(StatusCode, Json<ErrorResponse>)> {
        self.replication.as_ref().map(|_| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorResponse::with_code(
                    format!(
                        "{feature} is not available over the HTTP API in replicated mode; use the \
                         PostgreSQL wire protocol port (writes route through consensus there)"
                    ),
                    "58000",
                )),
            )
        })
    }
}

/// Map an execution error to an HTTP response, translating a structured
/// consensus [`SqlError`] (the replicated-write path's NOT_LEADER / staleness
/// / FATAL surface) onto idiomatic HTTP semantics (#5410). The mapping mirrors
/// the wire path's SQLSTATEs:
///
/// - **NotLeader** (`25006`) → **421 Misdirected Request** with the leader hint
///   in the [`LEADER_HINT_HEADER`] response header (and the detail/hint in the
///   body). 421 is the HTTP "you reached the wrong node, retarget" status — the
///   HTTP-shaped equivalent of the wire redirect contract. The leader address
///   lets a client redirect without re-probing the cluster.
/// - **StalenessExceeded / ReadTimeout** (`57P03`) → **503 Service Unavailable**
///   with `Retry-After: 1` — a retryable "can't serve this right now".
/// - **FatalApply** (`58000`) → **503 Service Unavailable** — this node halted
///   and must be restarted to resync.
/// - **Any other structured SqlError** → **500 Internal Server Error**.
/// - **A plain (non-structured) executor error** — e.g. a deterministic
///   constraint/SQL rejection, identical on every replica — → **400 Bad
///   Request** with the SQL error message, the same status standalone uses.
pub(crate) fn execution_error_response(err: &anyhow::Error) -> axum::response::Response {
    use axum::http::HeaderValue;

    let Some(sql_error) = err.downcast_ref::<SqlError>() else {
        // Not a consensus refusal: a deterministic SQL/constraint error (or
        // any standalone executor error). 400, exactly like standalone.
        return (StatusCode::BAD_REQUEST, Json(ErrorResponse::new(err.to_string())))
            .into_response();
    };

    let body = Json(ErrorResponse {
        error: sql_error_message(sql_error),
        code: Some(sql_error.code.to_string()),
    });

    match sql_error.code {
        SQLSTATE_NOT_LEADER => {
            // 421 Misdirected Request: wrong node, retarget to the leader.
            let mut resp = (StatusCode::MISDIRECTED_REQUEST, body).into_response();
            if let Some(detail) = &sql_error.detail {
                if let Ok(value) = HeaderValue::from_str(detail) {
                    resp.headers_mut().insert(LEADER_HINT_HEADER, value);
                }
            }
            resp
        }
        // Retryable: staleness/RYW timeout or a halted node.
        SQLSTATE_RETRY | SQLSTATE_FATAL => {
            let mut resp = (StatusCode::SERVICE_UNAVAILABLE, body).into_response();
            resp.headers_mut().insert("Retry-After", HeaderValue::from_static("1"));
            resp
        }
        // Any other structured error (internal_error catch-all, etc.).
        _ => (StatusCode::INTERNAL_SERVER_ERROR, body).into_response(),
    }
}

/// Flatten a [`SqlError`]'s message + detail + hint into one human-readable
/// string for the JSON error body (HTTP has no separate DETAIL/HINT fields
/// the way the PostgreSQL wire protocol does).
fn sql_error_message(e: &SqlError) -> String {
    let mut msg = e.message.clone();
    if let Some(detail) = &e.detail {
        msg.push_str(" — ");
        msg.push_str(detail);
    }
    if let Some(hint) = &e.hint {
        msg.push_str(" (");
        msg.push_str(hint);
        msg.push(')');
    }
    msg
}

/// Create the HTTP API router
///
/// # Arguments
/// * `db` - Legacy database reference for backwards compatibility (subscriptions, table listing)
/// * `registry` - Database registry for shared database access
/// * `subscription_manager` - Subscription manager for real-time updates
/// * `metrics` - Optional server metrics for observability
pub fn create_http_router(
    db: Arc<Database>,
    registry: DatabaseRegistry,
    subscription_manager: Arc<SubscriptionManager>,
    metrics: Option<ServerMetrics>,
    replication: Option<StdArc<ReplicationHandle>>,
) -> Router {
    let state = HttpState {
        registry: registry.clone(),
        db: db.clone(),
        subscription_manager: subscription_manager.clone(),
        metrics,
        replication,
    };
    let is_replicated = state.replication.is_some();

    // Create main router with state
    let main_router = Router::new()
        .route("/health", get(health_check))
        .route("/api/query", post(execute_query))
        .route("/api/subscribe", get(subscribe_stream))
        .route("/api/tables", get(list_tables))
        .route("/api/tables/{table_name}", get(get_table_info))
        // CRUD endpoints for auto-generated RESTful access
        .route("/api/tables/{table_name}/rows", get(super::crud::list_rows))
        .route("/api/tables/{table_name}/rows", post(super::crud::create_row))
        .route("/api/tables/{table_name}/rows/{id}", get(super::crud::get_row))
        .route("/api/tables/{table_name}/rows/{id}", put(super::crud::update_row))
        .route("/api/tables/{table_name}/rows/{id}", patch(super::crud::patch_row))
        .route("/api/tables/{table_name}/rows/{id}", delete(super::crud::delete_row))
        // GraphQL endpoint
        .route("/api/graphql", post(graphql_handler))
        // Stats endpoints
        .route("/stats/subscriptions/efficiency", get(get_efficiency_stats))
        .with_state(state);

    // Create storage sub-router with its own state. The blob storage API
    // also writes to the local registry database, so in replicated mode it
    // is gated to a uniform 503 rather than nested (#5393).
    if is_replicated {
        let gated = Router::new().fallback(|| async {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorResponse::with_code(
                    "the blob storage API is not available over HTTP in replicated mode; use the \
                     PostgreSQL wire protocol port",
                    "58000",
                )),
            )
        });
        main_router.nest("/api/storage", gated)
    } else {
        let storage_router = super::storage::create_storage_router(db, registry);
        main_router.nest("/api/storage", storage_router)
    }
}

/// Extract database name from request headers, falling back to default
pub fn get_database_name(headers: &HeaderMap) -> String {
    headers
        .get(DATABASE_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| DEFAULT_DATABASE_NAME.to_string())
}

/// GraphQL endpoint handler with relationship resolution support.
///
/// In replicated mode the GraphQL surface is no longer gated (#5421): both
/// queries and mutations route through consensus via the [`HttpState::session`]
/// choke point — a GraphQL **query** lowers to a SELECT served from the
/// replicated state machine, and a GraphQL **mutation** lowers to an
/// INSERT/UPDATE/DELETE proposed through consensus (leader-only,
/// freeze-at-propose). A mutation on a follower surfaces the NOT_LEADER refusal
/// as a `421` with a leader hint (via [`execution_error_response`]) and is never
/// executed against the follower's local database (the split-brain invariant).
/// Schema/relationship introspection reads the **applied replicated catalog**
/// (via [`ReplicationHandle::schema_snapshot`]) rather than the (empty) local
/// registry database. The GraphQL→SQL lowering parameterizes every value arg
/// (`graphql::graphql_to_sql`), so string args cannot inject SQL. In standalone
/// mode every step is identical to before (the schema comes from the local
/// catalog, the session is the local executor).
async fn graphql_handler(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Json(req): Json<graphql::GraphQLRequest>,
) -> impl IntoResponse {
    debug!("Received GraphQL request: {}", req.query);

    // Get the database name from headers
    let db_name = get_database_name(&headers);

    // Get or create the shared database from the registry
    let shared_db = state.registry.get_or_create(&db_name).await;

    // Resolve the schema map (table → TableSchema) used for both introspection
    // and relationship resolution. In replicated mode it comes from the applied
    // consensus catalog; in standalone mode from the local registry database.
    let schemas = state.graphql_schema_map(&shared_db).await;

    // Introspection (__schema / __type) is answered directly from the schema
    // map — no SQL executes.
    if let Some(introspection_result) = graphql::try_introspection_query(&schemas, &req.query) {
        return (
            StatusCode::OK,
            Json(graphql::GraphQLResponse { data: Some(introspection_result), errors: None }),
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

    // Check if we have nested fields that need relationship resolution
    let has_nested = graphql::has_nested_fields(&query_info);

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

    // Inline the bound value params (`$1`, `$2`, …) into the SQL as escaped
    // literals before execution (#5421). The GraphQL→SQL lowering parameterizes
    // every value arg, but the replicated write path proposes a SQL *string*
    // through consensus (it carries no param vector), and the replicated session
    // rejects the prepared-statement path outright. Rendering the params with
    // the same `ToSql` escaping the wire `Bind`/`EXECUTE` paths use keeps string
    // args injection-safe (a quote in a value is doubled, never breaks out) while
    // producing a self-contained statement that executes identically standalone
    // and over consensus.
    let sql = crate::session::substitute_named_params(&sql, &params);

    debug!("Generated SQL: {}", sql);

    // Build the session through the replicated choke point: in replicated mode
    // queries read the state machine and mutations propose through consensus;
    // in standalone mode it is the ordinary local-executor session (#5421).
    let mut session = state.session(&db_name, shared_db.clone());

    // Execute the main query (params are already inlined as literals above).
    let result = session.execute(&sql).await;

    match result {
        Ok(exec_result) => {
            match exec_result {
                crate::session::ExecutionResult::Select { rows, columns } => {
                    let column_names: Vec<String> =
                        columns.iter().map(|c| c.name.clone()).collect();
                    let row_values: Vec<Vec<_>> = rows
                        .iter()
                        .map(|r| r.values.iter().map(super::types::sql_value_to_json).collect())
                        .collect();

                    let mut rows_json: Vec<serde_json::Map<String, serde_json::Value>> = row_values
                        .iter()
                        .map(|row| {
                            let mut obj = serde_json::Map::new();
                            for (col, val) in column_names.iter().zip(row.iter()) {
                                obj.insert(col.clone(), val.clone());
                            }
                            obj
                        })
                        .collect();

                    // If we have nested fields, resolve relationships using the
                    // schema map resolved above (replicated catalog in
                    // replicated mode, local catalog in standalone mode).
                    if has_nested {
                        if let graphql::GraphQLQueryInfo::Query {
                            table_name, nested_fields, ..
                        } = &query_info
                        {
                            if !schemas.is_empty() {
                                let ctx = graphql::GraphQLExecutionContext::new(&schemas);
                                let nested_queries =
                                    graphql::build_nested_queries(&ctx, table_name, nested_fields);

                                // Execute nested queries and attach results
                                for nested in &nested_queries {
                                    if let Err(e) = execute_nested_query(
                                        &mut session,
                                        &mut rows_json,
                                        nested,
                                        &ctx,
                                    )
                                    .await
                                    {
                                        debug!("Warning: nested query failed: {}", e);
                                    }
                                }
                            }
                        }
                    }

                    let rows_json_values: Vec<serde_json::Value> =
                        rows_json.into_iter().map(serde_json::Value::Object).collect();

                    let response = graphql::GraphQLResponse {
                        data: Some(json!({
                            "data": rows_json_values
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
            // Map a consensus refusal of a GraphQL mutation/query onto the same
            // HTTP semantics the SQL surfaces use (#5421): NOT_LEADER → 421 with
            // the leader-hint header, staleness/FATAL → 503, any other consensus
            // error → 500, and a deterministic SQL rejection → 400. The body is
            // a GraphQL `errors` array either way, so a GraphQL client still
            // parses the failure; the status code + leader hint give a smart
            // client the redirect contract. In standalone mode every error is a
            // plain executor error → 400, exactly as before.
            graphql_execution_error_response(&e)
        }
    }
}

/// Map a GraphQL execution error onto an HTTP response with a GraphQL-shaped
/// body. Reuses [`execution_error_response`]'s consensus→HTTP mapping (status
/// code + `X-VibeSQL-Leader` header for NOT_LEADER) but replaces the
/// `ErrorResponse` JSON body with a [`graphql::GraphQLResponse`] carrying the
/// flattened message in its `errors` array, so the response is valid GraphQL
/// regardless of the status code (#5421).
fn graphql_execution_error_response(err: &anyhow::Error) -> axum::response::Response {
    // Derive the status + headers from the shared consensus mapping.
    let mut mapped = execution_error_response(err);
    let status = mapped.status();
    let leader_hint = mapped.headers_mut().remove(LEADER_HINT_HEADER);
    let retry_after = mapped.headers_mut().remove("Retry-After");

    let message = match err.downcast_ref::<SqlError>() {
        Some(sql_error) => sql_error_message(sql_error),
        None => format!("Query execution failed: {err}"),
    };

    let body = Json(graphql::GraphQLResponse {
        data: None,
        errors: Some(vec![graphql::GraphQLError::new(message)]),
    });
    let mut resp = (status, body).into_response();
    if let Some(hint) = leader_hint {
        resp.headers_mut().insert(LEADER_HINT_HEADER, hint);
    }
    if let Some(retry) = retry_after {
        resp.headers_mut().insert("Retry-After", retry);
    }
    resp
}

/// Build a map of table schemas from the database
fn build_schema_map(
    db: &vibesql_storage::Database,
) -> std::collections::HashMap<String, vibesql_catalog::TableSchema> {
    let mut schemas = std::collections::HashMap::new();
    for table_name in db.list_tables() {
        if let Some(table) = db.get_table(&table_name) {
            schemas.insert(table_name, table.schema.clone());
        }
    }
    schemas
}

/// Execute a nested query and attach results to parent rows
async fn execute_nested_query(
    session: &mut crate::session::Session,
    parent_rows: &mut [serde_json::Map<String, serde_json::Value>],
    nested: &graphql::NestedQueryInfo,
    _ctx: &graphql::GraphQLExecutionContext<'_>,
) -> Result<(), String> {
    if parent_rows.is_empty() {
        return Ok(());
    }

    // Get the key column from parent rows for the IN clause
    let key_column = match nested.direction {
        graphql::RelationshipDirection::OneToMany => {
            nested.pk_columns.first().ok_or("Missing PK column")?
        }
        graphql::RelationshipDirection::ManyToOne => {
            nested.fk_columns.first().ok_or("Missing FK column")?
        }
    };

    // Extract key values from parent rows
    let parent_values: Vec<serde_json::Value> = parent_rows
        .iter()
        .filter_map(|row| {
            row.iter().find(|(k, _)| k.eq_ignore_ascii_case(key_column)).map(|(_, v)| v.clone())
        })
        .collect();

    if parent_values.is_empty() {
        return Ok(());
    }

    // Generate and execute the nested query
    let sql = graphql::generate_nested_query_sql(nested, &parent_values)?;
    if sql.is_empty() {
        return Ok(());
    }

    debug!("Executing nested query: {}", sql);

    let result = session.execute(&sql).await.map_err(|e| format!("Nested query failed: {}", e))?;

    // Convert results to JSON objects
    let nested_rows: Vec<serde_json::Map<String, serde_json::Value>> = match result {
        crate::session::ExecutionResult::Select { rows, columns } => {
            let column_names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
            rows.iter()
                .map(|r| {
                    let mut obj = serde_json::Map::new();
                    for (col, val) in column_names.iter().zip(r.values.iter()) {
                        obj.insert(col.clone(), super::types::sql_value_to_json(val));
                    }
                    obj
                })
                .collect()
        }
        _ => return Ok(()),
    };

    // Execute any deeper nested queries recursively
    let mut nested_rows_mut = nested_rows;
    for deeper_nested in &nested.nested {
        if let Err(e) =
            Box::pin(execute_nested_query(session, &mut nested_rows_mut, deeper_nested, _ctx)).await
        {
            debug!("Warning: deeper nested query failed: {}", e);
        }
    }

    // Group results by the key column
    let grouped = graphql::group_nested_results(nested_rows_mut, nested);

    // Attach to parent rows
    graphql::attach_nested_results(parent_rows, nested, grouped);

    Ok(())
}

/// Health check endpoint.
///
/// Standalone mode: always `200 {"status":"ok"}`. Replicated mode (#5393):
/// reports the node's consensus role, writability, applied index, and any
/// fatal-halt reason, and returns `503` (with the same JSON body) whenever
/// the node cannot currently serve writes — so a load balancer routes
/// writes to the leader and routes around halted/lagging nodes even with a
/// status-code-only health check.
async fn health_check(State(state): State<HttpState>) -> impl IntoResponse {
    let version = env!("CARGO_PKG_VERSION").to_string();

    match &state.replication {
        None => (
            StatusCode::OK,
            Json(HealthResponse { status: "ok".to_string(), version, replication: None }),
        ),
        Some(handle) => {
            let snap = handle.health_snapshot();
            let status = if snap.can_serve_writes { "ok" } else { "unavailable" };
            let code = if snap.can_serve_writes {
                StatusCode::OK
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            };
            (
                code,
                Json(HealthResponse {
                    status: status.to_string(),
                    version,
                    replication: Some(ReplicationStatus {
                        node_id: snap.node_id,
                        role: role_str(snap.role).to_string(),
                        can_serve_writes: snap.can_serve_writes,
                        applied_index: snap.applied_index,
                        leader_id: snap.leader_id,
                        fatal_reason: snap.fatal_reason,
                    }),
                }),
            )
        }
    }
}

/// Get subscription partial update efficiency statistics
async fn get_efficiency_stats(State(state): State<HttpState>) -> impl IntoResponse {
    if let Some(metrics) = &state.metrics {
        let stats = metrics.get_efficiency_stats();
        (StatusCode::OK, Json(stats)).into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, Json(ErrorResponse::new("Metrics not available")))
            .into_response()
    }
}

/// Execute a SQL query with optional pagination
async fn execute_query(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Json(req): Json<QueryRequest>,
) -> impl IntoResponse {
    debug!("Executing query: {} (limit: {:?}, offset: {:?})", req.sql, req.limit, req.offset);

    // In replicated mode this builds a replicated session (#5410): writes
    // propose through consensus (leader-only, freeze-at-propose), reads run
    // against the replicated state machine, and a write on a follower surfaces
    // as 421 with a leader hint — never executed against the unreplicated
    // local database. In standalone mode it is the ordinary local session.

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

    // Get the database name from headers
    let db_name = get_database_name(&headers);

    // Get or create the shared database from the registry
    let shared_db = state.registry.get_or_create(&db_name).await;

    // Create a session — replicated when the server is in replicated mode.
    let mut session = state.session(&db_name, shared_db);

    // Execute the query
    let result = if params.is_empty() {
        session.execute(&req.sql).await
    } else {
        session.execute_with_params(&req.sql, &params).await
    };

    match result {
        Ok(exec_result) => {
            match exec_result {
                crate::session::ExecutionResult::Select { rows, columns } => {
                    let column_names: Vec<String> =
                        columns.iter().map(|c| c.name.clone()).collect();
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
            // Map a consensus refusal (NotLeader → 421 + leader hint, staleness
            // / FATAL → 503) or a deterministic SQL rejection (→ 400) to the
            // right HTTP status (#5410).
            execution_error_response(&e)
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
        let pk_columns: Vec<&String> =
            schema.primary_key.as_ref().map(|pk| pk.iter().collect()).unwrap_or_default();

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
    /// Enable selective column updates (default: true if not specified)
    #[serde(default)]
    pub selective_enabled: Option<bool>,
    /// Minimum columns that must change to use selective update (default: 1)
    #[serde(default)]
    pub selective_min_changed_columns: Option<usize>,
    /// Maximum ratio of changed columns before falling back to full row (default: 0.5)
    /// E.g., 0.3 means if >30% of columns changed, send full row instead
    #[serde(default)]
    pub selective_max_changed_ratio: Option<f64>,
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
    State(state): State<HttpState>,
    headers: HeaderMap,
    Query(params): Query<SubscribeQuery>,
) -> axum::response::Response {
    use axum::response::sse::{Event, KeepAlive, Sse};

    debug!("SSE subscription requested for query: {}", params.query);

    // Subscriptions are fed by change events from the local registry
    // database, which receives no replicated writes — gate them in
    // replicated mode (feeding subscriptions from applied consensus entries
    // is follow-on #5410).
    if let Some((code, body)) = state.replicated_gate("subscriptions") {
        return (code, body).into_response();
    }

    // Get the database name from headers
    let db_name = get_database_name(&headers);

    // Get or create the shared database from the registry
    let shared_db = state.registry.get_or_create(&db_name).await;

    // Parse optional parameters
    let params_vec = if let Some(params_str) = params.params {
        let mut values = Vec::new();
        for s in params_str.split(',') {
            use vibesql_types::SqlValue;
            let val = if let Ok(i) = s.trim().parse::<i64>() {
                SqlValue::Integer(i)
            } else {
                SqlValue::Varchar(arcstr::ArcStr::from(s.trim()))
            };
            values.push(val);
        }
        values
    } else {
        vec![]
    };

    // Validate selective updates parameters BEFORE query execution (fail-fast)
    // This ensures clients get clear parameter validation errors, not mixed with query errors
    if let Some(max_ratio) = params.selective_max_changed_ratio {
        if !(0.0..=1.0).contains(&max_ratio) {
            error!("Invalid selective_max_changed_ratio: {}", max_ratio);
            let event_data = serde_json::to_string(&SseEvent {
                event_type: "error".to_string(),
                columns: None,
                rows: None,
                old: None,
                new: None,
                error: Some("selective_max_changed_ratio must be between 0.0 and 1.0".to_string()),
            })
            .unwrap_or_default();

            let stream = futures::stream::once(async move {
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(Event::default().data(event_data))
            });

            return Sse::new(stream).keep_alive(KeepAlive::default()).into_response();
        }
    }

    // Execute initial query with the shared database
    let mut session =
        crate::session::Session::new(db_name.clone(), "http_user".to_string(), shared_db);

    // Execute the initial query
    let result = if params_vec.is_empty() {
        session.execute(&params.query).await
    } else {
        session.execute_with_params(&params.query, &params_vec).await
    };

    // Validate it's a SELECT statement
    let columns = match result {
        Ok(crate::session::ExecutionResult::Select { rows: _, columns }) => {
            columns.iter().map(|c| c.name.clone()).collect::<Vec<_>>()
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
            })
            .unwrap_or_default();

            let stream = futures::stream::once(async move {
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(Event::default().data(event_data))
            });

            return Sse::new(stream).keep_alive(KeepAlive::default()).into_response();
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
            })
            .unwrap_or_default();

            let stream = futures::stream::once(async move {
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(Event::default().data(event_data))
            });

            return Sse::new(stream).keep_alive(KeepAlive::default()).into_response();
        }
    };

    // Build selective updates config from query parameters
    let mut selective_config = SelectiveColumnConfig::default();
    if let Some(enabled) = params.selective_enabled {
        selective_config.enabled = enabled;
    }
    if let Some(min_changed) = params.selective_min_changed_columns {
        selective_config.min_changed_columns = min_changed;
    }
    if let Some(max_ratio) = params.selective_max_changed_ratio {
        // Already validated in early validation block above
        selective_config.max_changed_columns_ratio = max_ratio;
    }

    // Create subscription via SubscriptionManager
    let (tx, mut rx) = mpsc::channel(32);
    let subscription_id = match state.subscription_manager.subscribe(params.query.clone(), tx) {
        Ok(id) => {
            // Track the new subscription in metrics
            if let Some(ref metrics) = state.metrics {
                metrics.increment_subscriptions_active();
            }
            id
        }
        Err(e) => {
            error!("Failed to create subscription: {}", e);
            let event_data = serde_json::to_string(&SseEvent {
                event_type: "error".to_string(),
                columns: None,
                rows: None,
                old: None,
                new: None,
                error: Some(format!("Failed to create subscription: {}", e)),
            })
            .unwrap_or_default();

            let stream = futures::stream::once(async move {
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(Event::default().data(event_data))
            });

            return Sse::new(stream).keep_alive(KeepAlive::default()).into_response();
        }
    };

    // Apply selective updates configuration to the subscription
    state.subscription_manager.update_selective_updates(subscription_id, selective_config);

    // Detect PK columns for selective column updates
    // Parse the subscription query to detect primary key columns
    if let Ok(stmt) = vibesql_parser::Parser::parse_sql(&params.query) {
        // Get the database for PK detection
        let db_for_pk = state.registry.get_or_create(&db_name).await;
        let db_guard_pk = db_for_pk.read().await;
        let pk_detection = detect_pk_columns_from_stmt(&stmt, &db_guard_pk);
        drop(db_guard_pk);

        debug!(
            subscription_id = %subscription_id,
            pk_columns = ?pk_detection.pk_column_indices,
            confident = pk_detection.confident,
            "Detected PK columns for HTTP SSE subscription"
        );

        // Update subscription with PK columns and eligibility
        let newly_eligible = state.subscription_manager.update_pk_columns_with_eligibility(
            subscription_id,
            pk_detection.pk_column_indices,
            pk_detection.confident,
        );

        // Track selective-eligible metric
        if newly_eligible {
            if let Some(ref metrics) = state.metrics {
                metrics.increment_selective_eligible();
            }
        }
    }

    // Send initial results using the database from the registry
    let columns_clone = columns.clone();
    // Re-get the database from registry and send initial results
    let db_for_initial = state.registry.get_or_create(&db_name).await;
    let db_guard = db_for_initial.read().await;
    if let Err(e) =
        state.subscription_manager.send_initial_results(subscription_id, &db_guard).await
    {
        drop(db_guard);
        error!("Failed to send initial results: {}", e);
        let was_selective_eligible = state.subscription_manager.unsubscribe(subscription_id);
        if let Some(ref metrics) = state.metrics {
            metrics.decrement_subscriptions_active();
            if was_selective_eligible {
                metrics.decrement_selective_eligible();
            }
        }

        let event_data = serde_json::to_string(&SseEvent {
            event_type: "error".to_string(),
            columns: None,
            rows: None,
            old: None,
            new: None,
            error: Some(format!("Failed to send initial results: {}", e)),
        })
        .unwrap_or_default();

        let stream = futures::stream::once(async move {
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(Event::default().data(event_data))
        });

        return Sse::new(stream).keep_alive(KeepAlive::default()).into_response();
    }
    drop(db_guard); // Release the read lock before entering the streaming loop

    // Create stream that receives updates from subscription and converts to SSE events
    let stream = async_stream::stream! {
        let mut is_first_event = true;
        while let Some(update) = rx.recv().await {
            match update {
                SubscriptionUpdate::Full { rows, .. } => {
                    // Convert rows to JSON
                    let row_values: Vec<Vec<serde_json::Value>> = rows
                        .iter()
                        .map(|r| r.values.iter().map(super::types::sql_value_to_json).collect())
                        .collect();

                    // First Full event is the initial result, subsequent ones are updates
                    let event_type_str = if is_first_event {
                        is_first_event = false;
                        "initial"
                    } else {
                        // Record full update sent for efficiency stats (non-initial updates only)
                        if let Some(ref metrics) = state.metrics {
                            metrics.record_full_update_sent();
                        }
                        "update"
                    };

                    let event_data = match serde_json::to_string(&SseEvent {
                        event_type: event_type_str.to_string(),
                        columns: Some(columns_clone.clone()),
                        rows: Some(row_values),
                        old: None,
                        new: None,
                        error: None,
                    }) {
                        Ok(data) => data,
                        Err(e) => {
                            error!("Failed to serialize update event: {}", e);
                            continue;
                        }
                    };

                    yield Ok::<_, Box<dyn std::error::Error + Send + Sync>>(
                        Event::default().event(event_type_str).data(event_data)
                    );
                }
                SubscriptionUpdate::Delta { inserts, updates, deletes, .. } => {
                    let insert_rows: Vec<Vec<serde_json::Value>> = inserts
                        .iter()
                        .map(|r| r.values.iter().map(super::types::sql_value_to_json).collect())
                        .collect();

                    let update_pairs: Vec<(Vec<serde_json::Value>, Vec<serde_json::Value>)> = updates
                        .iter()
                        .map(|(old, new)| {
                            let old_vals = old.values.iter().map(super::types::sql_value_to_json).collect();
                            let new_vals = new.values.iter().map(super::types::sql_value_to_json).collect();
                            (old_vals, new_vals)
                        })
                        .collect();

                    let delete_rows: Vec<Vec<serde_json::Value>> = deletes
                        .iter()
                        .map(|r| r.values.iter().map(super::types::sql_value_to_json).collect())
                        .collect();

                    let event_data = match serde_json::to_string(&json!({
                        "type": "delta",
                        "inserts": insert_rows,
                        "updates": update_pairs,
                        "deletes": delete_rows,
                    })) {
                        Ok(data) => data,
                        Err(e) => {
                            error!("Failed to serialize delta event: {}", e);
                            continue;
                        }
                    };

                    yield Ok::<_, Box<dyn std::error::Error + Send + Sync>>(
                        Event::default().event("delta").data(event_data)
                    );
                }
                SubscriptionUpdate::Partial { updates, .. } => {
                    // Record partial update sent for efficiency stats
                    if let Some(ref metrics) = state.metrics {
                        metrics.record_partial_update_sent();
                    }

                    // Send partial updates (only changed columns + PK columns)
                    let partial_updates: Vec<serde_json::Value> = updates
                        .iter()
                        .map(|partial| {
                            json!({
                                "columns": partial.column_indices,
                                "old": partial.old_values.iter().map(super::types::sql_value_to_json).collect::<Vec<_>>(),
                                "new": partial.new_values.iter().map(super::types::sql_value_to_json).collect::<Vec<_>>(),
                            })
                        })
                        .collect();

                    let event_data = match serde_json::to_string(&json!({
                        "type": "partial",
                        "updates": partial_updates,
                    })) {
                        Ok(data) => data,
                        Err(e) => {
                            error!("Failed to serialize partial event: {}", e);
                            continue;
                        }
                    };

                    yield Ok::<_, Box<dyn std::error::Error + Send + Sync>>(
                        Event::default().event("partial").data(event_data)
                    );
                }
                SubscriptionUpdate::Error { message, .. } => {
                    let event_data = match serde_json::to_string(&SseEvent {
                        event_type: "error".to_string(),
                        columns: None,
                        rows: None,
                        old: None,
                        new: None,
                        error: Some(message),
                    }) {
                        Ok(data) => data,
                        Err(e) => {
                            error!("Failed to serialize error event: {}", e);
                            continue;
                        }
                    };

                    yield Ok::<_, Box<dyn std::error::Error + Send + Sync>>(
                        Event::default().event("error").data(event_data)
                    );

                    // Stop stream on error
                    break;
                }
            }
        }

        // Clean up subscription when stream ends
        let was_selective_eligible = state.subscription_manager.unsubscribe(subscription_id);
        if let Some(ref metrics) = state.metrics {
            metrics.decrement_subscriptions_active();
            if was_selective_eligible {
                metrics.decrement_selective_eligible();
            }
        }
    };

    // Create SSE response with keepalive
    Sse::new(stream).keep_alive(KeepAlive::default()).into_response()
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
        let rows = vec![vec![serde_json::json!("a")], vec![serde_json::json!("b")]];

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
