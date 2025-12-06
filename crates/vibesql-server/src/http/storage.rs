//! HTTP API endpoints for blob storage operations
//!
//! This module provides REST endpoints for uploading, downloading,
//! and managing blobs via HTTP.

use std::sync::Arc;

use axum::{
    body::Bytes,
    extract::{Path, State},
    http::{header, HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{delete, get, post},
    Json, Router,
};
use tokio::sync::RwLock;
use tracing::{debug, error};

use vibesql_storage::{BlobId, BlobStorageConfig, BlobStorageService, Database};

use super::types::*;
use crate::registry::DatabaseRegistry;

/// State for storage endpoints
#[derive(Clone)]
pub struct StorageState {
    /// Database registry for shared database access
    pub registry: DatabaseRegistry,
    /// Legacy database reference for backwards compatibility (blob storage)
    pub db: Arc<RwLock<Database>>,
    /// Blob storage service
    pub blob_service: Arc<BlobStorageService>,
}

impl StorageState {
    /// Create storage state from database and registry
    pub fn new(db: Arc<Database>, registry: DatabaseRegistry) -> Self {
        // Create blob service with the database reference
        let blob_service = Arc::new(BlobStorageService::new_default(db.clone()));
        // Wrap Database in RwLock for safe concurrent access
        let db_inner = Arc::try_unwrap(db).unwrap_or_else(|arc| (*arc).clone());
        let db = Arc::new(RwLock::new(db_inner));
        Self { registry, db, blob_service }
    }

    /// Create storage state with custom config
    #[allow(dead_code)]
    pub fn with_config(config: BlobStorageConfig, db: Arc<Database>, registry: DatabaseRegistry) -> Self {
        // Create blob service with the database reference
        let blob_service = Arc::new(BlobStorageService::new(config, db.clone()));
        // Wrap Database in RwLock for safe concurrent access
        let db_inner = Arc::try_unwrap(db).unwrap_or_else(|arc| (*arc).clone());
        let db = Arc::new(RwLock::new(db_inner));
        Self { registry, db, blob_service }
    }
}

/// Create the storage API router
pub fn create_storage_router(db: Arc<Database>, registry: DatabaseRegistry) -> Router {
    let state = StorageState::new(db, registry);

    Router::new()
        .route("/upload", post(upload_blob))
        .route("/{blob_id}", get(download_blob))
        .route("/{blob_id}", delete(delete_blob))
        .route("/{blob_id}/metadata", get(get_blob_metadata))
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

/// Tests for blob storage HTTP handlers.
/// These tests require the memory storage backend to be enabled.
#[cfg(all(test, feature = "opendal", feature = "storage-memory"))]
mod tests {
    use super::*;
    use axum::{body::Body, http::Request};
    use tower::ServiceExt;

    fn create_test_router() -> Router {
        let db = Arc::new(Database::new());
        let registry = DatabaseRegistry::new();
        let config = BlobStorageConfig {
            backend: "memory".to_string(),
            config: serde_json::json!({}),
        };
        let state = StorageState::with_config(config, db, registry);

        Router::new()
            .route("/upload", post(upload_blob))
            .route("/{blob_id}", get(download_blob))
            .route("/{blob_id}", delete(delete_blob))
            .route("/{blob_id}/metadata", get(get_blob_metadata))
            .with_state(state)
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
}
