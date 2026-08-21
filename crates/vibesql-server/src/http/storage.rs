//! HTTP API endpoints for blob storage operations
//!
//! This module provides REST endpoints for uploading, downloading,
//! and managing blobs via HTTP.
//!
//! # Replicated vs. standalone storage (#5455)
//!
//! The blob storage API has two backends selected by the server mode:
//!
//! * **Standalone**: blobs are written to a separate [`BlobStorageService`]
//!   byte store (OpenDAL — filesystem, S3, GCS, Azure, in-memory). This path is
//!   unchanged: the bytes live outside the SQL database and never touch
//!   consensus.
//!
//! * **Replicated**: a separate node-local byte store would never replicate, so
//!   blob writes are instead routed through the **same consensus SQL write
//!   path** every other replicated HTTP surface uses. Each blob is a row in the
//!   replicated `__vibesql_blobs` system table `(id, content_type, size,
//!   created_at, data BLOB)`. An upload proposes an `INSERT` through consensus
//!   via the [`HttpState::session`](crate::http::rest::HttpState) choke point
//!   (leader-only, freeze-at-propose); a delete proposes a `DELETE`; a download
//!   / metadata read runs the `SELECT` against the replicated state machine. So
//!   a blob written on the leader replicates to every follower and is readable
//!   on any node, an upload/delete on a follower surfaces the `NOT_LEADER`
//!   refusal as `421` (never written locally — the split-brain invariant), and
//!   there is no local-only blob write. The blob bytes ride the Raft log as a
//!   SQL `BLOB` literal; this is sound for modest blobs but large blobs would
//!   bloat the log/snapshots, so replicated uploads are capped at
//!   [`MAX_REPLICATED_BLOB_BYTES`] (streaming large blobs out-of-band is a
//!   documented follow-on).

use std::sync::Arc;

use axum::{
    body::Bytes,
    extract::{Path, State},
    http::{header, HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{delete, get, post},
    Json, Router,
};
use tracing::{debug, error};
use vibesql_storage::{
    shared_metadata_db, BlobId, BlobStorageConfig, BlobStorageService, Database, SharedMetadataDb,
};

use super::rest::{execution_error_response, get_database_name, HttpState};
use super::types::*;
use crate::registry::DatabaseRegistry;

/// Replicated system table that holds blobs as rows so blob writes ride the
/// consensus SQL write path (#5455).
const BLOB_TABLE: &str = "__vibesql_blobs";

/// Upper bound on a blob uploaded in replicated mode. Replicated blobs flow
/// through the Raft log (and into snapshots) as a SQL `BLOB` literal, so a very
/// large blob would bloat the log; cap at a conservative size and reject larger
/// uploads with a clear error. Streaming large blobs out-of-band (e.g. an
/// external object store referenced by a replicated row) is a documented
/// follow-on.
pub const MAX_REPLICATED_BLOB_BYTES: usize = 8 * 1024 * 1024;

/// State for the standalone storage endpoints (separate byte store).
#[derive(Clone)]
pub struct StorageState {
    /// Database registry for shared database access
    pub registry: DatabaseRegistry,
    /// Database handle backing the standalone blob metadata surface.
    ///
    /// This is the **same** shared handle held by [`Self::blob_service`], not a
    /// separate copy: blob metadata written by the service (the
    /// `vibesql_storage` system table) is immediately visible through this
    /// field, and vice versa. See [`SharedMetadataDb`].
    pub db: SharedMetadataDb,
    /// Blob storage service
    pub blob_service: Arc<BlobStorageService>,
}

impl StorageState {
    /// Create storage state from database and registry
    pub fn new(db: Arc<Database>, registry: DatabaseRegistry) -> Self {
        Self::with_config(BlobStorageConfig::default(), db, registry)
    }

    /// Create storage state with custom config
    pub fn with_config(
        config: BlobStorageConfig,
        db: Arc<Database>,
        registry: DatabaseRegistry,
    ) -> Self {
        // Wrap the database in a shared, lock-guarded handle **once** and give
        // the blob service a clone of that same handle, so metadata the
        // service persists is visible through `self.db` (and to anything else
        // holding a clone).
        //
        // Previously the service was handed `db.clone()` and internally did
        // `Arc::try_unwrap(...).unwrap_or_else(|arc| (*arc).clone())`. Because
        // this constructor kept its own `db` alive, the strong count was never
        // 1, so that always produced a private `Database` clone — and
        // `Database::clone()` resets `persistence_engine`/`change_sender` to
        // `None`, so blob metadata went into a disconnected copy nothing else
        // could see (PR #6446 review).
        //
        // The single `try_unwrap`-or-clone below is the pre-existing boundary
        // between the caller's lock-free `Arc<Database>` (`HttpState::db`) and
        // the lock-guarded handle this state needs in order to write at all.
        // Removing it means making the standalone HTTP database a shared
        // lock-guarded handle end to end (`HttpState`, subscriptions, the
        // registry) — tracked in #6448.
        let db_inner = Arc::try_unwrap(db).unwrap_or_else(|arc| (*arc).clone());
        let db = shared_metadata_db(db_inner);
        let blob_service = Arc::new(BlobStorageService::new(config, Arc::clone(&db)));
        Self { registry, db, blob_service }
    }
}

/// Create the storage API router for **standalone** mode (separate byte store).
pub fn create_storage_router(db: Arc<Database>, registry: DatabaseRegistry) -> Router {
    let state = StorageState::new(db, registry);

    Router::new()
        .route("/upload", post(upload_blob))
        .route("/{blob_id}", get(download_blob))
        .route("/{blob_id}", delete(delete_blob))
        .route("/{blob_id}/metadata", get(get_blob_metadata))
        .with_state(state)
}

/// Create the storage API router for **replicated** mode (#5455).
///
/// Blob writes route through consensus (the `__vibesql_blobs` table) via the
/// shared [`HttpState::session`] choke point, exactly like the other replicated
/// HTTP surfaces, so they replicate to every node, are readable on any node, and
/// a write on a follower is refused with `421` rather than written locally.
pub fn create_replicated_storage_router(state: HttpState) -> Router {
    Router::new()
        .route("/upload", post(replicated_upload_blob))
        .route("/{blob_id}", get(replicated_download_blob))
        .route("/{blob_id}", delete(replicated_delete_blob))
        .route("/{blob_id}/metadata", get(replicated_get_blob_metadata))
        .with_state(state)
}

/// Upload a blob
///
/// POST /api/storage/upload
///
/// Accepts raw binary data in the request body.
/// Content-Type header is used to determine the blob's MIME type.
///
/// Returns JSON with blob metadata including the generated ID.
async fn upload_blob(
    State(state): State<StorageState>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    // Get content type from header, default to application/octet-stream
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_string();

    let size = body.len() as i64;

    debug!("Uploading blob: {} bytes, content-type: {}", size, content_type);

    match state.blob_service.store(body, content_type.clone()).await {
        Ok(blob_id) => {
            let url = state.blob_service.get_url(&blob_id);
            let response = BlobUploadResponse { id: blob_id.to_string(), size, content_type, url };
            (StatusCode::CREATED, Json(response)).into_response()
        }
        Err(e) => {
            error!("Failed to upload blob: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::new(format!("Failed to upload blob: {}", e))),
            )
                .into_response()
        }
    }
}

/// Download a blob
///
/// GET /api/storage/{blob_id}
///
/// Returns the raw binary data with appropriate Content-Type header.
async fn download_blob(
    State(state): State<StorageState>,
    Path(blob_id): Path<String>,
) -> impl IntoResponse {
    // Parse blob ID
    let id = match BlobId::parse(&blob_id) {
        Some(id) => id,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::new(format!("Invalid blob ID: {}", blob_id))),
            )
                .into_response();
        }
    };

    debug!("Downloading blob: {}", id);

    // Get metadata first to determine content type
    let content_type = match state.blob_service.get_metadata(&id).await {
        Ok(metadata) => metadata.content_type,
        Err(e) => {
            debug!("Failed to get metadata for blob {}, using default content-type: {}", id, e);
            "application/octet-stream".to_string()
        }
    };

    // Get blob data
    match state.blob_service.get(&id).await {
        Ok(data) => {
            let mut headers = HeaderMap::new();
            headers.insert(
                header::CONTENT_TYPE,
                content_type
                    .parse()
                    .unwrap_or(header::HeaderValue::from_static("application/octet-stream")),
            );
            headers.insert(header::CONTENT_LENGTH, data.len().to_string().parse().unwrap());

            (StatusCode::OK, headers, data).into_response()
        }
        Err(e) => {
            error!("Failed to download blob {}: {}", id, e);
            (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse::new(format!("Blob not found: {}", blob_id))),
            )
                .into_response()
        }
    }
}

/// Get blob metadata
///
/// GET /api/storage/{blob_id}/metadata
///
/// Returns JSON with blob metadata (id, size, content_type, created_at).
async fn get_blob_metadata(
    State(state): State<StorageState>,
    Path(blob_id): Path<String>,
) -> impl IntoResponse {
    // Parse blob ID
    let id = match BlobId::parse(&blob_id) {
        Some(id) => id,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::new(format!("Invalid blob ID: {}", blob_id))),
            )
                .into_response();
        }
    };

    debug!("Getting metadata for blob: {}", id);

    match state.blob_service.get_metadata(&id).await {
        Ok(metadata) => {
            let response = BlobMetadataResponse {
                id: metadata.id.to_string(),
                size: metadata.size,
                content_type: metadata.content_type,
                created_at: metadata.created_at.to_rfc3339(),
            };
            (StatusCode::OK, Json(response)).into_response()
        }
        Err(e) => {
            error!("Failed to get metadata for blob {}: {}", id, e);
            (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse::new(format!("Blob not found: {}", blob_id))),
            )
                .into_response()
        }
    }
}

/// Delete a blob
///
/// DELETE /api/storage/{blob_id}
///
/// Returns 204 No Content on success.
async fn delete_blob(
    State(state): State<StorageState>,
    Path(blob_id): Path<String>,
) -> impl IntoResponse {
    // Parse blob ID
    let id = match BlobId::parse(&blob_id) {
        Some(id) => id,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::new(format!("Invalid blob ID: {}", blob_id))),
            )
                .into_response();
        }
    };

    debug!("Deleting blob: {}", id);

    match state.blob_service.delete(&id).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => {
            error!("Failed to delete blob {}: {}", id, e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::new(format!("Failed to delete blob: {}", e))),
            )
                .into_response()
        }
    }
}

// ===========================================================================
// Replicated blob handlers (#5455) — blobs as rows in the consensus state
// machine, routed through the shared `HttpState::session` choke point.
// ===========================================================================

/// Render bytes as a SQL `BLOB` literal (`X'..'`). Hex digits cannot break out
/// of the literal, so this is injection-safe for arbitrary blob bytes.
fn blob_literal(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2 + 3);
    s.push_str("X'");
    for b in bytes {
        s.push_str(&format!("{:02X}", b));
    }
    s.push('\'');
    s
}

/// Escape a value for a single-quoted SQL string literal (double any quote).
fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// Ensure the replicated `__vibesql_blobs` table exists, creating it through
/// consensus on first use. `CREATE TABLE IF NOT EXISTS` is idempotent and
/// deterministic, so it is safe to propose on every upload.
async fn ensure_blob_table(state: &HttpState, db_name: &str) -> Result<(), anyhow::Error> {
    let shared_db = state.registry.get_or_create(db_name).await;
    let mut session = state.session(db_name, shared_db);
    let ddl = format!(
        "CREATE TABLE IF NOT EXISTS {BLOB_TABLE} (\
            id VARCHAR(64) PRIMARY KEY, \
            content_type VARCHAR(255), \
            size BIGINT, \
            created_at VARCHAR(64), \
            data BLOB)"
    );
    session.execute(&ddl).await?;
    Ok(())
}

/// Upload a blob in replicated mode: propose an INSERT through consensus.
async fn replicated_upload_blob(
    State(state): State<HttpState>,
    headers: HeaderMap,
    body: Bytes,
) -> axum::response::Response {
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_string();

    let size = body.len() as i64;

    if body.len() > MAX_REPLICATED_BLOB_BYTES {
        return (
            StatusCode::PAYLOAD_TOO_LARGE,
            Json(ErrorResponse::new(format!(
                "blob is {} bytes; replicated blob uploads are capped at {} bytes (large blobs \
                 would bloat the Raft log/snapshots)",
                body.len(),
                MAX_REPLICATED_BLOB_BYTES
            ))),
        )
            .into_response();
    }

    let db_name = get_database_name(&headers);

    // Create the blob table through consensus if needed.
    if let Err(e) = ensure_blob_table(&state, &db_name).await {
        return execution_error_response(&e);
    }

    let id = BlobId::new();
    let created_at = chrono::Utc::now().to_rfc3339();

    debug!("Uploading replicated blob {}: {} bytes, content-type: {}", id, size, content_type);

    let sql = format!(
        "INSERT INTO {BLOB_TABLE} (id, content_type, size, created_at, data) VALUES ({}, {}, {}, {}, {})",
        sql_string_literal(&id.to_string()),
        sql_string_literal(&content_type),
        size,
        sql_string_literal(&created_at),
        blob_literal(&body),
    );

    let shared_db = state.registry.get_or_create(&db_name).await;
    let mut session = state.session(&db_name, shared_db);
    match session.execute(&sql).await {
        Ok(_) => {
            let url = format!("/api/storage/{}", id);
            let response = BlobUploadResponse { id: id.to_string(), size, content_type, url };
            (StatusCode::CREATED, Json(response)).into_response()
        }
        Err(e) => {
            error!("Failed to upload replicated blob {}: {}", id, e);
            // NOT_LEADER → 421 (+ leader hint), staleness/FATAL → 503, a
            // deterministic SQL error → 400 — the same mapping the SQL/CRUD
            // surfaces use, so a follower upload is refused, never stored locally.
            execution_error_response(&e)
        }
    }
}

/// Read a single blob row's columns from the replicated state machine.
///
/// Returns `Ok(Some(..))` when the blob exists, `Ok(None)` when it does not (or
/// the table has not been created yet), and `Err` only on a consensus refusal.
async fn fetch_replicated_blob(
    state: &HttpState,
    db_name: &str,
    id: &BlobId,
) -> Result<Option<(String, i64, String, Vec<u8>)>, anyhow::Error> {
    let sql = format!(
        "SELECT content_type, size, created_at, data FROM {BLOB_TABLE} WHERE id = {}",
        sql_string_literal(&id.to_string()),
    );
    let shared_db = state.registry.get_or_create(db_name).await;
    let mut session = state.session(db_name, shared_db);
    let result = match session.execute(&sql).await {
        Ok(r) => r,
        Err(e) => {
            // A missing table reads as "no such blob", not a server error — the
            // table is created lazily on first upload. Any other error (e.g. a
            // consensus refusal) propagates.
            let msg = e.to_string().to_lowercase();
            if msg.contains("no such table")
                || msg.contains("does not exist")
                || msg.contains("not found")
                || msg.contains("unknown table")
            {
                return Ok(None);
            }
            return Err(e);
        }
    };

    let crate::session::ExecutionResult::Select { rows, .. } = result else {
        return Ok(None);
    };
    let Some(row) = rows.into_iter().next() else {
        return Ok(None);
    };

    use vibesql_types::SqlValue;
    let content_type = match row.values.first() {
        Some(SqlValue::Character(s)) | Some(SqlValue::Varchar(s)) => s.to_string(),
        _ => "application/octet-stream".to_string(),
    };
    let size = match row.values.get(1) {
        Some(SqlValue::Bigint(n)) => *n,
        Some(SqlValue::Integer(n)) => *n,
        _ => 0,
    };
    let created_at = match row.values.get(2) {
        Some(SqlValue::Character(s)) | Some(SqlValue::Varchar(s)) => s.to_string(),
        _ => String::new(),
    };
    let data = match row.values.get(3) {
        Some(SqlValue::Blob(b)) => b.clone(),
        _ => Vec::new(),
    };

    Ok(Some((content_type, size, created_at, data)))
}

/// Download a blob in replicated mode: read from the replicated state machine.
async fn replicated_download_blob(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path(blob_id): Path<String>,
) -> axum::response::Response {
    let id = match BlobId::parse(&blob_id) {
        Some(id) => id,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::new(format!("Invalid blob ID: {}", blob_id))),
            )
                .into_response();
        }
    };

    let db_name = get_database_name(&headers);
    debug!("Downloading replicated blob: {}", id);

    match fetch_replicated_blob(&state, &db_name, &id).await {
        Ok(Some((content_type, _size, _created_at, data))) => {
            let mut out_headers = HeaderMap::new();
            out_headers.insert(
                header::CONTENT_TYPE,
                content_type
                    .parse()
                    .unwrap_or(header::HeaderValue::from_static("application/octet-stream")),
            );
            out_headers.insert(header::CONTENT_LENGTH, data.len().to_string().parse().unwrap());
            (StatusCode::OK, out_headers, data).into_response()
        }
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse::new(format!("Blob not found: {}", blob_id))),
        )
            .into_response(),
        Err(e) => execution_error_response(&e),
    }
}

/// Get blob metadata in replicated mode: read from the replicated state machine.
async fn replicated_get_blob_metadata(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path(blob_id): Path<String>,
) -> axum::response::Response {
    let id = match BlobId::parse(&blob_id) {
        Some(id) => id,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::new(format!("Invalid blob ID: {}", blob_id))),
            )
                .into_response();
        }
    };

    let db_name = get_database_name(&headers);
    debug!("Getting metadata for replicated blob: {}", id);

    match fetch_replicated_blob(&state, &db_name, &id).await {
        Ok(Some((content_type, size, created_at, _data))) => {
            let response = BlobMetadataResponse {
                id: id.to_string(),
                size,
                content_type,
                created_at,
            };
            (StatusCode::OK, Json(response)).into_response()
        }
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse::new(format!("Blob not found: {}", blob_id))),
        )
            .into_response(),
        Err(e) => execution_error_response(&e),
    }
}

/// Delete a blob in replicated mode: propose a DELETE through consensus.
async fn replicated_delete_blob(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path(blob_id): Path<String>,
) -> axum::response::Response {
    let id = match BlobId::parse(&blob_id) {
        Some(id) => id,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::new(format!("Invalid blob ID: {}", blob_id))),
            )
                .into_response();
        }
    };

    let db_name = get_database_name(&headers);
    debug!("Deleting replicated blob: {}", id);

    // Create the table if it does not exist so a delete-before-any-upload is
    // idempotent (204), matching the standalone delete semantics.
    if let Err(e) = ensure_blob_table(&state, &db_name).await {
        return execution_error_response(&e);
    }

    let sql = format!(
        "DELETE FROM {BLOB_TABLE} WHERE id = {}",
        sql_string_literal(&id.to_string()),
    );

    let shared_db = state.registry.get_or_create(&db_name).await;
    let mut session = state.session(&db_name, shared_db);
    match session.execute(&sql).await {
        // Delete is idempotent — deleting a non-existent blob still succeeds.
        Ok(_) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => {
            error!("Failed to delete replicated blob {}: {}", id, e);
            execution_error_response(&e)
        }
    }
}

/// Tests for blob storage HTTP handlers.
/// These tests require the memory storage backend to be enabled.
#[cfg(all(test, feature = "opendal", feature = "storage-memory"))]
mod tests {
    use axum::{body::Body, http::Request};
    use tower::ServiceExt;

    use super::*;

    fn create_test_state() -> StorageState {
        let db = Arc::new(Database::new());
        let registry = DatabaseRegistry::new();
        let config =
            BlobStorageConfig { backend: "memory".to_string(), config: serde_json::json!({}) };
        StorageState::with_config(config, db, registry)
    }

    fn router_for(state: StorageState) -> Router {
        Router::new()
            .route("/upload", post(upload_blob))
            .route("/{blob_id}", get(download_blob))
            .route("/{blob_id}", delete(delete_blob))
            .route("/{blob_id}/metadata", get(get_blob_metadata))
            .with_state(state)
    }

    fn create_test_router() -> Router {
        router_for(create_test_state())
    }

    #[tokio::test]
    async fn test_upload_blob_success() {
        // Test that blob upload succeeds and returns CREATED status
        let router = create_test_router();

        let request = Request::builder()
            .method("POST")
            .uri("/upload")
            .header("content-type", "text/plain")
            .body(Body::from("Hello, World!"))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();

        // The store method stores metadata and returns a blob ID
        assert_eq!(response.status(), StatusCode::CREATED);
    }

    #[tokio::test]
    async fn test_download_nonexistent_blob() {
        let router = create_test_router();

        let request = Request::builder()
            .method("GET")
            .uri("/550e8400-e29b-41d4-a716-446655440000")
            .body(Body::empty())
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_download_invalid_blob_id() {
        let router = create_test_router();

        let request =
            Request::builder().method("GET").uri("/invalid-id").body(Body::empty()).unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_get_metadata_nonexistent_blob() {
        let router = create_test_router();

        let request = Request::builder()
            .method("GET")
            .uri("/550e8400-e29b-41d4-a716-446655440000/metadata")
            .body(Body::empty())
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_delete_blob_nonexistent() {
        let router = create_test_router();

        let request = Request::builder()
            .method("DELETE")
            .uri("/550e8400-e29b-41d4-a716-446655440000")
            .body(Body::empty())
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        // Delete is idempotent - deleting non-existent blob returns 204
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    /// End-to-end round trip for issue #6443: upload a blob over HTTP,
    /// confirm `GET /{id}/metadata` returns the real size/content_type
    /// (previously always 404 "Blob not found", even right after a
    /// successful upload), confirm `GET /{id}` preserves the uploaded
    /// content type (previously always fell back to
    /// application/octet-stream), then confirm metadata is gone after
    /// `DELETE /{id}`.
    #[tokio::test]
    async fn test_upload_metadata_download_delete_round_trip() {
        let router = create_test_router();

        let upload_request = Request::builder()
            .method("POST")
            .uri("/upload")
            .header("content-type", "text/plain")
            .body(Body::from("Hello, metadata!"))
            .unwrap();
        let upload_response = router.clone().oneshot(upload_request).await.unwrap();
        assert_eq!(upload_response.status(), StatusCode::CREATED);

        let body = axum::body::to_bytes(upload_response.into_body(), usize::MAX).await.unwrap();
        let uploaded: BlobUploadResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(uploaded.size, "Hello, metadata!".len() as i64);
        assert_eq!(uploaded.content_type, "text/plain");

        // GET /{id}/metadata should now return real metadata instead of 404.
        let metadata_request = Request::builder()
            .method("GET")
            .uri(format!("/{}/metadata", uploaded.id))
            .body(Body::empty())
            .unwrap();
        let metadata_response = router.clone().oneshot(metadata_request).await.unwrap();
        assert_eq!(metadata_response.status(), StatusCode::OK);
        let metadata_body =
            axum::body::to_bytes(metadata_response.into_body(), usize::MAX).await.unwrap();
        let metadata: BlobMetadataResponse = serde_json::from_slice(&metadata_body).unwrap();
        assert_eq!(metadata.id, uploaded.id);
        assert_eq!(metadata.size, "Hello, metadata!".len() as i64);
        assert_eq!(metadata.content_type, "text/plain");
        assert!(!metadata.created_at.is_empty());

        // GET /{id} should preserve the real content type (not the
        // application/octet-stream fallback).
        let download_request = Request::builder()
            .method("GET")
            .uri(format!("/{}", uploaded.id))
            .body(Body::empty())
            .unwrap();
        let download_response = router.clone().oneshot(download_request).await.unwrap();
        assert_eq!(download_response.status(), StatusCode::OK);
        assert_eq!(download_response.headers().get(header::CONTENT_TYPE).unwrap(), "text/plain");

        // DELETE /{id} removes both bytes and metadata.
        let delete_request = Request::builder()
            .method("DELETE")
            .uri(format!("/{}", uploaded.id))
            .body(Body::empty())
            .unwrap();
        let delete_response = router.clone().oneshot(delete_request).await.unwrap();
        assert_eq!(delete_response.status(), StatusCode::NO_CONTENT);

        let metadata_after_delete_request = Request::builder()
            .method("GET")
            .uri(format!("/{}/metadata", uploaded.id))
            .body(Body::empty())
            .unwrap();
        let metadata_after_delete_response =
            router.oneshot(metadata_after_delete_request).await.unwrap();
        assert_eq!(metadata_after_delete_response.status(), StatusCode::NOT_FOUND);
    }

    /// Regression test for PR #6446's review finding: an upload through the
    /// HTTP surface must write blob metadata into the database handle
    /// `StorageState` itself holds — the shared instance — not into a private
    /// `Database` clone owned by the blob service.
    ///
    /// This is the production shape: `StorageState` keeps `state.db` alive
    /// while the blob service holds a clone of the same handle, so the handle
    /// always has multiple live references. Under the previous
    /// `Arc::try_unwrap(...).unwrap_or_else(clone)` constructor the service
    /// silently operated on its own disconnected copy and this assertion would
    /// fail (the caller's database never even grew the `vibesql_storage`
    /// table).
    #[tokio::test]
    async fn test_upload_persists_metadata_into_shared_state_database() {
        let state = create_test_state();
        // The service and `state.db` must be the same underlying instance.
        assert!(Arc::strong_count(&state.db) >= 2);
        let caller_db = Arc::clone(&state.db);
        let router = router_for(state);

        let request = Request::builder()
            .method("POST")
            .uri("/upload")
            .header("content-type", "text/plain")
            .body(Body::from("Hello, shared state!"))
            .unwrap();
        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let uploaded: BlobUploadResponse = serde_json::from_slice(&body).unwrap();

        // Read the metadata back through the caller's handle.
        let db = caller_db.read();
        let table = db
            .get_table("vibesql_storage")
            .expect("shared state database must see the vibesql_storage metadata table");
        let ids: Vec<String> = table
            .scan()
            .iter()
            .filter_map(|row| match row.get(0) {
                Some(vibesql_types::SqlValue::Varchar(v)) => Some(v.to_string()),
                _ => None,
            })
            .collect();
        assert!(
            ids.contains(&uploaded.id),
            "uploaded blob id {} missing from shared state database (found {:?})",
            uploaded.id,
            ids
        );
    }

    #[test]
    fn test_blob_literal_roundtrip_safe() {
        // Quotes/backslashes cannot break out of an X'' hex literal.
        assert_eq!(blob_literal(&[0x00, 0xFF, 0x41]), "X'00FF41'");
        assert_eq!(blob_literal(&[]), "X''");
    }

    #[test]
    fn test_sql_string_literal_escapes_quotes() {
        assert_eq!(sql_string_literal("a'b"), "'a''b'");
        assert_eq!(sql_string_literal("plain"), "'plain'");
    }
}
