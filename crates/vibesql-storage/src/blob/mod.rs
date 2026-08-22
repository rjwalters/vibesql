/// Blob/file storage module for VibeSql
///
/// This module provides abstraction for storing and retrieving binary data
/// with metadata tracking in the database.
use chrono::Utc;
use serde::{Deserialize, Serialize};

pub mod id;
pub mod service;

pub use id::BlobId;
pub use service::BlobStorageService;

/// Metadata for a stored blob
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlobMetadata {
    /// Unique identifier for the blob
    pub id: BlobId,

    /// Size in bytes
    pub size: i64,

    /// MIME type (e.g., "image/png", "application/json")
    pub content_type: String,

    /// When the blob was created
    pub created_at: chrono::DateTime<Utc>,

    /// Optional custom metadata (JSON)
    pub metadata: Option<serde_json::Value>,
}

impl BlobMetadata {
    /// Create new blob metadata
    pub fn new(id: BlobId, size: i64, content_type: String) -> Self {
        Self { id, size, content_type, created_at: Utc::now(), metadata: None }
    }

    /// Add custom metadata
    pub fn with_metadata(mut self, metadata: serde_json::Value) -> Self {
        self.metadata = Some(metadata);
        self
    }
}

/// Configuration for blob storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlobStorageConfig {
    /// Backend type: "fs", "s3", "gcs", "azure", etc.
    pub backend: String,

    /// Backend-specific configuration (root path, bucket, etc.)
    pub config: serde_json::Value,
}

impl Default for BlobStorageConfig {
    fn default() -> Self {
        Self {
            backend: "fs".to_string(),
            config: serde_json::json!({
                "root": "/var/vibesql/storage"
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_blob_id_creation() {
        let id = BlobId::new();
        assert!(!id.to_string().is_empty());
    }

    #[test]
    fn test_blob_metadata_creation() {
        let id = BlobId::new();
        let meta = BlobMetadata::new(id.clone(), 1024, "text/plain".to_string());
        assert_eq!(meta.size, 1024);
        assert_eq!(meta.content_type, "text/plain");
    }
}
