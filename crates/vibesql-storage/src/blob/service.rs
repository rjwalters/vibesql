use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use chrono::{Datelike, TimeZone, Timelike, Utc};
#[cfg(feature = "opendal")]
use opendal::{services, Operator};
use parking_lot::RwLock;
use vibesql_types::{DataType, SqlValue};

/// Blob storage service
///
/// Manages storing and retrieving blobs with support for multiple backends
/// via OpenDAL abstraction layer.
use super::{BlobId, BlobMetadata, BlobStorageConfig};
use crate::{
    database::Database,
    error::{StorageError, StorageResult},
    Row,
};

/// Name of the system table used to persist blob metadata (id, size,
/// content_type, created_at, custom metadata JSON). Auto-created on first
/// `store_metadata()` call. See issue #3482 / #6443.
const BLOB_METADATA_TABLE: &str = "vibesql_storage";

/// Blob storage service for file/blob operations
pub struct BlobStorageService {
    #[cfg(feature = "opendal")]
    operator: Option<Operator>,
    config: BlobStorageConfig,
    /// Metadata database, guarded for interior mutability.
    ///
    /// `store_metadata`/`get_metadata`/`delete()` need `&mut Database` to
    /// create the `vibesql_storage` system table and insert/delete rows in
    /// it, but every public method on this service (`store`, `get`,
    /// `delete`, ...) takes `&self` (the service is shared via `Arc` across
    /// concurrent HTTP handlers). A `parking_lot::RwLock` gives interior
    /// mutability without making every call site `&mut self`; the guard is
    /// always dropped before any `.await` point, so it never blocks across
    /// suspension.
    db: RwLock<Database>,
}

impl BlobStorageService {
    /// Create a new blob storage service
    #[cfg(feature = "opendal")]
    pub fn new(config: BlobStorageConfig, db: Arc<Database>) -> Self {
        let operator = Self::create_operator(&config).ok();
        let db = Arc::try_unwrap(db).unwrap_or_else(|arc| (*arc).clone());
        Self { operator, config, db: RwLock::new(db) }
    }

    /// Create a new blob storage service (non-opendal build)
    #[cfg(not(feature = "opendal"))]
    pub fn new(config: BlobStorageConfig, db: Arc<Database>) -> Self {
        let db = Arc::try_unwrap(db).unwrap_or_else(|arc| (*arc).clone());
        Self { config, db: RwLock::new(db) }
    }

    /// Create with default configuration (local filesystem)
    pub fn new_default(db: Arc<Database>) -> Self {
        Self::new(BlobStorageConfig::default(), db)
    }

    /// Create an OpenDAL operator based on the configuration
    #[cfg(feature = "opendal")]
    fn create_operator(config: &BlobStorageConfig) -> StorageResult<Operator> {
        match config.backend.as_str() {
            #[cfg(feature = "storage-fs")]
            "fs" | "filesystem" => Self::create_fs_operator(config),

            #[cfg(feature = "storage-s3")]
            "s3" => Self::create_s3_operator(config),

            #[cfg(feature = "storage-gcs")]
            "gcs" => Self::create_gcs_operator(config),

            #[cfg(feature = "storage-azure")]
            "azure" | "azblob" => Self::create_azure_operator(config),

            #[cfg(feature = "storage-memory")]
            "memory" => Self::create_memory_operator(),

            backend => Err(StorageError::Other(format!(
                "Unknown or unsupported storage backend: '{}'. Enable the appropriate feature flag (storage-fs, storage-s3, storage-gcs, storage-azure, storage-memory).",
                backend
            ))),
        }
    }

    /// Create filesystem operator
    #[cfg(all(feature = "opendal", feature = "storage-fs"))]
    fn create_fs_operator(config: &BlobStorageConfig) -> StorageResult<Operator> {
        let root =
            config.config.get("root").and_then(|v| v.as_str()).unwrap_or("/var/vibesql/storage");

        let builder = services::Fs::default().root(root);

        Operator::new(builder).map_err(|e| {
            StorageError::Other(format!("Failed to create filesystem operator: {}", e))
        })
    }

    /// Create S3-compatible operator (works with AWS S3, MinIO, Cloudflare R2, etc.)
    #[cfg(all(feature = "opendal", feature = "storage-s3"))]
    fn create_s3_operator(config: &BlobStorageConfig) -> StorageResult<Operator> {
        let bucket = config.config.get("bucket").and_then(|v| v.as_str()).ok_or_else(|| {
            StorageError::Other("S3 backend requires 'bucket' configuration".to_string())
        })?;

        let mut builder = services::S3::default().bucket(bucket);

        // Optional configurations
        if let Some(endpoint) = config.config.get("endpoint").and_then(|v| v.as_str()) {
            builder = builder.endpoint(endpoint);
        }

        if let Some(region) = config.config.get("region").and_then(|v| v.as_str()) {
            builder = builder.region(region);
        }

        if let Some(access_key_id) = config.config.get("access_key_id").and_then(|v| v.as_str()) {
            builder = builder.access_key_id(access_key_id);
        }

        if let Some(secret_access_key) =
            config.config.get("secret_access_key").and_then(|v| v.as_str())
        {
            builder = builder.secret_access_key(secret_access_key);
        }

        if let Some(root) = config.config.get("root").and_then(|v| v.as_str()) {
            builder = builder.root(root);
        }

        Operator::new(builder)
            .map_err(|e| StorageError::Other(format!("Failed to create S3 operator: {}", e)))
    }

    /// Create Google Cloud Storage operator
    #[cfg(all(feature = "opendal", feature = "storage-gcs"))]
    fn create_gcs_operator(config: &BlobStorageConfig) -> StorageResult<Operator> {
        let bucket = config.config.get("bucket").and_then(|v| v.as_str()).ok_or_else(|| {
            StorageError::Other("GCS backend requires 'bucket' configuration".to_string())
        })?;

        let mut builder = services::Gcs::default().bucket(bucket);

        if let Some(credential) = config.config.get("credential").and_then(|v| v.as_str()) {
            builder = builder.credential(credential);
        }

        if let Some(credential_path) = config.config.get("credential_path").and_then(|v| v.as_str())
        {
            builder = builder.credential_path(credential_path);
        }

        if let Some(root) = config.config.get("root").and_then(|v| v.as_str()) {
            builder = builder.root(root);
        }

        Operator::new(builder)
            .map_err(|e| StorageError::Other(format!("Failed to create GCS operator: {}", e)))
    }

    /// Create Azure Blob Storage operator
    #[cfg(all(feature = "opendal", feature = "storage-azure"))]
    fn create_azure_operator(config: &BlobStorageConfig) -> StorageResult<Operator> {
        let container =
            config.config.get("container").and_then(|v| v.as_str()).ok_or_else(|| {
                StorageError::Other("Azure backend requires 'container' configuration".to_string())
            })?;

        let mut builder = services::Azblob::default().container(container);

        if let Some(account_name) = config.config.get("account_name").and_then(|v| v.as_str()) {
            builder = builder.account_name(account_name);
        }

        if let Some(account_key) = config.config.get("account_key").and_then(|v| v.as_str()) {
            builder = builder.account_key(account_key);
        }

        if let Some(endpoint) = config.config.get("endpoint").and_then(|v| v.as_str()) {
            builder = builder.endpoint(endpoint);
        }

        if let Some(root) = config.config.get("root").and_then(|v| v.as_str()) {
            builder = builder.root(root);
        }

        Operator::new(builder)
            .map_err(|e| StorageError::Other(format!("Failed to create Azure operator: {}", e)))
    }

    /// Create in-memory operator (useful for testing)
    #[cfg(all(feature = "opendal", feature = "storage-memory"))]
    fn create_memory_operator() -> StorageResult<Operator> {
        let builder = services::Memory::default();

        Operator::new(builder)
            .map_err(|e| StorageError::Other(format!("Failed to create memory operator: {}", e)))
    }

    /// Store a blob and return its ID
    #[cfg(feature = "opendal")]
    pub async fn store(&self, data: Bytes, content_type: String) -> StorageResult<BlobId> {
        let id = BlobId::new();
        let size = data.len() as i64;
        let path = id.to_path();

        // Write to backend using OpenDAL operator
        if let Some(ref op) = self.operator {
            op.write(&path, data.to_vec())
                .await
                .map_err(|e| StorageError::Other(format!("Failed to write blob: {}", e)))?;
        } else {
            return Err(StorageError::Other(
                "Storage operator not initialized. Check your configuration.".to_string(),
            ));
        }

        // Store metadata in database
        let metadata = BlobMetadata::new(id.clone(), size, content_type);
        self.store_metadata(&metadata).await?;

        Ok(id)
    }

    /// Store a blob and return its ID (non-opendal build - stub)
    #[cfg(not(feature = "opendal"))]
    pub async fn store(&self, _data: Bytes, _content_type: String) -> StorageResult<BlobId> {
        Err(StorageError::Other(
            "Blob storage requires the 'opendal' feature to be enabled".to_string(),
        ))
    }

    /// Retrieve a blob by ID
    #[cfg(feature = "opendal")]
    pub async fn get(&self, id: &BlobId) -> StorageResult<Bytes> {
        let path = id.to_path();

        if let Some(ref op) = self.operator {
            let data = op
                .read(&path)
                .await
                .map_err(|e| StorageError::Other(format!("Failed to read blob {}: {}", id, e)))?;
            Ok(Bytes::from(data.to_vec()))
        } else {
            Err(StorageError::Other(
                "Storage operator not initialized. Check your configuration.".to_string(),
            ))
        }
    }

    /// Retrieve a blob by ID (non-opendal build - stub)
    #[cfg(not(feature = "opendal"))]
    pub async fn get(&self, id: &BlobId) -> StorageResult<Bytes> {
        Err(StorageError::Other(format!(
            "Blob storage requires the 'opendal' feature to be enabled (blob: {})",
            id
        )))
    }

    /// Get metadata for a blob
    ///
    /// Queries the `vibesql_storage` system table for the row matching `id`.
    /// Returns an error if the table doesn't exist yet (nothing has ever been
    /// stored) or no row matches `id`.
    pub async fn get_metadata(&self, id: &BlobId) -> StorageResult<BlobMetadata> {
        let db = self.db.read();
        let Some(table) = db.get_table(BLOB_METADATA_TABLE) else {
            return Err(StorageError::Other(format!("blob metadata not found: {}", id)));
        };

        let id_str = id.to_string();
        for row in table.scan() {
            let Some(SqlValue::Varchar(row_id)) = row.get(0) else { continue };
            if row_id.as_str() != id_str {
                continue;
            }

            let size = match row.get(1) {
                Some(SqlValue::Bigint(s)) => *s,
                _ => return Err(StorageError::Other("invalid blob metadata: size".to_string())),
            };
            let content_type = match row.get(2) {
                Some(SqlValue::Varchar(ct)) => ct.to_string(),
                _ => {
                    return Err(StorageError::Other(
                        "invalid blob metadata: content_type".to_string(),
                    ))
                }
            };
            let created_at = match row.get(3) {
                Some(SqlValue::Timestamp(ts)) => timestamp_to_datetime(ts),
                _ => {
                    return Err(StorageError::Other(
                        "invalid blob metadata: created_at".to_string(),
                    ))
                }
            };
            let metadata = match row.get(4) {
                Some(SqlValue::Varchar(json_str)) => serde_json::from_str(json_str).ok(),
                _ => None,
            };

            return Ok(BlobMetadata { id: id.clone(), size, content_type, created_at, metadata });
        }

        Err(StorageError::Other(format!("blob metadata not found: {}", id)))
    }

    /// Delete a blob
    #[cfg(feature = "opendal")]
    pub async fn delete(&self, id: &BlobId) -> StorageResult<()> {
        let path = id.to_path();

        // Delete from backend using OpenDAL operator
        if let Some(ref op) = self.operator {
            op.delete(&path)
                .await
                .map_err(|e| StorageError::Other(format!("Failed to delete blob {}: {}", id, e)))?;
        } else {
            return Err(StorageError::Other(
                "Storage operator not initialized. Check your configuration.".to_string(),
            ));
        }

        // Delete metadata from database. Idempotent by design (mirrors the
        // OpenDAL byte-delete semantics above): deleting a blob whose
        // metadata was never stored (or already deleted) is not an error.
        self.delete_metadata(id);
        Ok(())
    }

    /// Delete a blob (non-opendal build - stub)
    #[cfg(not(feature = "opendal"))]
    pub async fn delete(&self, id: &BlobId) -> StorageResult<()> {
        Err(StorageError::Other(format!(
            "Blob storage requires the 'opendal' feature to be enabled (blob: {})",
            id
        )))
    }

    /// Check if a blob exists
    #[cfg(feature = "opendal")]
    pub async fn exists(&self, id: &BlobId) -> StorageResult<bool> {
        let path = id.to_path();

        if let Some(ref op) = self.operator {
            match op.stat(&path).await {
                Ok(_) => Ok(true),
                Err(e) if e.kind() == opendal::ErrorKind::NotFound => Ok(false),
                Err(e) => {
                    Err(StorageError::Other(format!("Failed to check blob existence: {}", e)))
                }
            }
        } else {
            Err(StorageError::Other(
                "Storage operator not initialized. Check your configuration.".to_string(),
            ))
        }
    }

    /// Check if a blob exists (non-opendal build - stub)
    #[cfg(not(feature = "opendal"))]
    pub async fn exists(&self, id: &BlobId) -> StorageResult<bool> {
        Err(StorageError::Other(format!(
            "Blob storage requires the 'opendal' feature to be enabled (blob: {})",
            id
        )))
    }

    /// Store blob metadata in database
    ///
    /// Auto-creates the `vibesql_storage` system table on first use, then
    /// inserts a row for `metadata`. See issue #3482 / #6443.
    #[allow(dead_code)]
    async fn store_metadata(&self, metadata: &BlobMetadata) -> StorageResult<()> {
        let mut db = self.db.write();

        if db.get_table(BLOB_METADATA_TABLE).is_none() {
            let schema = vibesql_catalog::TableSchema::new(
                BLOB_METADATA_TABLE.to_string(),
                vec![
                    vibesql_catalog::ColumnSchema::new(
                        "id".to_string(),
                        DataType::Varchar { max_length: Some(64) },
                        false,
                    ),
                    vibesql_catalog::ColumnSchema::new("size".to_string(), DataType::Bigint, false),
                    vibesql_catalog::ColumnSchema::new(
                        "content_type".to_string(),
                        DataType::Varchar { max_length: Some(255) },
                        true,
                    ),
                    vibesql_catalog::ColumnSchema::new(
                        "created_at".to_string(),
                        DataType::Timestamp { with_timezone: false },
                        false,
                    ),
                    vibesql_catalog::ColumnSchema::new(
                        "metadata".to_string(),
                        DataType::CharacterLargeObject,
                        true,
                    ),
                ],
            );
            db.create_table(schema)?;
        }

        let row = Row::from_vec(vec![
            SqlValue::Varchar(arcstr::ArcStr::from(metadata.id.to_string())),
            SqlValue::Bigint(metadata.size),
            SqlValue::Varchar(arcstr::ArcStr::from(metadata.content_type.clone())),
            SqlValue::Timestamp(datetime_to_timestamp(metadata.created_at)),
            metadata
                .metadata
                .as_ref()
                .map(|m| SqlValue::Varchar(arcstr::ArcStr::from(m.to_string())))
                .unwrap_or(SqlValue::Null),
        ]);
        db.insert_row(BLOB_METADATA_TABLE, row)?;

        Ok(())
    }

    /// Delete blob metadata from the `vibesql_storage` system table.
    ///
    /// Idempotent: a missing table (nothing ever stored) or a missing row
    /// (already deleted, or never stored) is silently a no-op rather than an
    /// error, matching the idempotent semantics of blob byte deletion.
    ///
    /// Only called from the `opendal`-gated `delete()`; the non-opendal
    /// stub build never stores metadata in the first place.
    #[allow(dead_code)]
    fn delete_metadata(&self, id: &BlobId) {
        let mut db = self.db.write();
        let Some(table) = db.get_table_mut(BLOB_METADATA_TABLE) else {
            return;
        };
        let id_str = id.to_string();
        table.rows_mut().retain(|row| {
            !matches!(row.get(0), Some(SqlValue::Varchar(row_id)) if row_id.as_str() == id_str)
        });
    }

    /// Generate a URL for accessing a blob
    ///
    /// For local filesystem, returns a relative path.
    /// For cloud storage, returns the storage-specific URL format.
    pub fn get_url(&self, id: &BlobId) -> String {
        match self.config.backend.as_str() {
            "fs" | "filesystem" => format!("/storage/blobs/{}", id.to_url_safe()),
            "s3" => {
                if let Some(bucket) = self.config.config.get("bucket").and_then(|v| v.as_str()) {
                    if let Some(endpoint) =
                        self.config.config.get("endpoint").and_then(|v| v.as_str())
                    {
                        format!("{}/{}/{}", endpoint, bucket, id.to_path())
                    } else {
                        format!("s3://{}/{}", bucket, id.to_path())
                    }
                } else {
                    format!("s3://unknown/{}", id.to_path())
                }
            }
            "gcs" => {
                if let Some(bucket) = self.config.config.get("bucket").and_then(|v| v.as_str()) {
                    format!("gs://{}/{}", bucket, id.to_path())
                } else {
                    format!("gs://unknown/{}", id.to_path())
                }
            }
            "azure" | "azblob" => {
                if let Some(container) =
                    self.config.config.get("container").and_then(|v| v.as_str())
                {
                    if let Some(account) =
                        self.config.config.get("account_name").and_then(|v| v.as_str())
                    {
                        format!(
                            "https://{}.blob.core.windows.net/{}/{}",
                            account,
                            container,
                            id.to_path()
                        )
                    } else {
                        format!("azure://{}/{}", container, id.to_path())
                    }
                } else {
                    format!("azure://unknown/{}", id.to_path())
                }
            }
            "memory" => format!("memory://{}", id.to_path()),
            _ => format!("/storage/blobs/{}", id.to_url_safe()),
        }
    }

    /// Generate a presigned URL for temporary access to a blob
    ///
    /// This is primarily useful for cloud backends (S3, GCS, Azure) where you want to
    /// give temporary, direct access to a blob without going through the application.
    #[cfg(feature = "opendal")]
    pub async fn get_presigned_url(
        &self,
        id: &BlobId,
        expires_in: Duration,
    ) -> StorageResult<String> {
        let path = id.to_path();

        if let Some(ref op) = self.operator {
            // Check if the operator supports presigning
            let presigned = op
                .presign_read(&path, expires_in)
                .await
                .map_err(|e| {
                    StorageError::Other(format!(
                        "Failed to generate presigned URL for blob {}: {}. Note: presigned URLs are only supported for cloud backends (S3, GCS, Azure).",
                        id, e
                    ))
                })?;

            Ok(presigned.uri().to_string())
        } else {
            Err(StorageError::Other(
                "Storage operator not initialized. Check your configuration.".to_string(),
            ))
        }
    }

    /// Generate a presigned URL (non-opendal build - stub)
    #[cfg(not(feature = "opendal"))]
    pub async fn get_presigned_url(
        &self,
        id: &BlobId,
        _expires_in: Duration,
    ) -> StorageResult<String> {
        Err(StorageError::Other(format!(
            "Presigned URLs require the 'opendal' feature to be enabled (blob: {})",
            id
        )))
    }

    /// Get the configured backend type
    pub fn backend(&self) -> &str {
        &self.config.backend
    }

    /// Check if the service is properly initialized
    #[cfg(feature = "opendal")]
    pub fn is_initialized(&self) -> bool {
        self.operator.is_some()
    }

    /// Check if the service is properly initialized (non-opendal build)
    #[cfg(not(feature = "opendal"))]
    pub fn is_initialized(&self) -> bool {
        false
    }
}

/// Convert a `chrono::DateTime<Utc>` to a `vibesql_types::Timestamp` for
/// storage in the `created_at` column of the `vibesql_storage` table.
fn datetime_to_timestamp(dt: chrono::DateTime<Utc>) -> vibesql_types::Timestamp {
    vibesql_types::Timestamp {
        date: vibesql_types::Date { year: dt.year(), month: dt.month() as u8, day: dt.day() as u8 },
        time: vibesql_types::Time {
            hour: dt.hour() as u8,
            minute: dt.minute() as u8,
            second: dt.second() as u8,
            nanosecond: dt.nanosecond(),
        },
    }
}

/// Convert a `vibesql_types::Timestamp` (read back from the `created_at`
/// column) to a `chrono::DateTime<Utc>`. Falls back to `Utc::now()` for a
/// date/time combination that (should never, but) fails to construct.
fn timestamp_to_datetime(ts: &vibesql_types::Timestamp) -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(
        ts.date.year,
        ts.date.month as u32,
        ts.date.day as u32,
        ts.time.hour as u32,
        ts.time.minute as u32,
        ts.time.second as u32,
    )
    .single()
    .map(|dt| dt + chrono::Duration::nanoseconds(ts.time.nanosecond as i64))
    .unwrap_or_else(Utc::now)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_blob_url_generation_fs() {
        let config = BlobStorageConfig::default();
        let db = Arc::new(Database::new());
        let service = BlobStorageService::new(config, db);

        let id = BlobId::new();
        let url = service.get_url(&id);
        assert!(url.starts_with("/storage/blobs/"));
    }

    #[test]
    fn test_blob_url_generation_s3() {
        let config = BlobStorageConfig {
            backend: "s3".to_string(),
            config: serde_json::json!({
                "bucket": "my-bucket",
                "region": "us-east-1"
            }),
        };
        let db = Arc::new(Database::new());
        let service = BlobStorageService::new(config, db);

        let id = BlobId::new();
        let url = service.get_url(&id);
        assert!(url.starts_with("s3://my-bucket/"));
    }

    #[test]
    fn test_blob_url_generation_gcs() {
        let config = BlobStorageConfig {
            backend: "gcs".to_string(),
            config: serde_json::json!({
                "bucket": "my-gcs-bucket"
            }),
        };
        let db = Arc::new(Database::new());
        let service = BlobStorageService::new(config, db);

        let id = BlobId::new();
        let url = service.get_url(&id);
        assert!(url.starts_with("gs://my-gcs-bucket/"));
    }

    #[test]
    fn test_blob_url_generation_azure() {
        let config = BlobStorageConfig {
            backend: "azure".to_string(),
            config: serde_json::json!({
                "container": "my-container",
                "account_name": "myaccount"
            }),
        };
        let db = Arc::new(Database::new());
        let service = BlobStorageService::new(config, db);

        let id = BlobId::new();
        let url = service.get_url(&id);
        assert!(url.starts_with("https://myaccount.blob.core.windows.net/my-container/"));
    }

    #[test]
    fn test_backend_accessor() {
        let config = BlobStorageConfig { backend: "s3".to_string(), config: serde_json::json!({}) };
        let db = Arc::new(Database::new());
        let service = BlobStorageService::new(config, db);

        assert_eq!(service.backend(), "s3");
    }

    #[cfg(all(feature = "opendal", feature = "storage-memory"))]
    #[tokio::test]
    async fn test_memory_backend_store_and_get() {
        let config =
            BlobStorageConfig { backend: "memory".to_string(), config: serde_json::json!({}) };
        let db = Arc::new(Database::new());
        let service = BlobStorageService::new(config, db);

        assert!(service.is_initialized());

        let data = Bytes::from("Hello, World!");
        let id = service.store(data.clone(), "text/plain".to_string()).await.unwrap();

        let retrieved = service.get(&id).await.unwrap();
        assert_eq!(retrieved, data);

        // Test exists
        assert!(service.exists(&id).await.unwrap());

        // Test delete
        service.delete(&id).await.unwrap();
        assert!(!service.exists(&id).await.unwrap());
    }

    /// End-to-end round trip for issue #6443: upload a blob, confirm
    /// `get_metadata()` returns the real size/content_type (not the
    /// hardcoded "not found" stub), then confirm the metadata row is gone
    /// after `delete()`.
    #[cfg(all(feature = "opendal", feature = "storage-memory"))]
    #[tokio::test]
    async fn test_metadata_store_get_delete_round_trip() {
        let config =
            BlobStorageConfig { backend: "memory".to_string(), config: serde_json::json!({}) };
        let db = Arc::new(Database::new());
        let service = BlobStorageService::new(config, db);

        let data = Bytes::from("Hello, metadata!");
        let content_type = "text/plain".to_string();
        let id = service.store(data.clone(), content_type.clone()).await.unwrap();

        // Immediately after upload, metadata should be retrievable with the
        // real size and content type (previously always errored: "blob
        // metadata not found").
        let metadata = service.get_metadata(&id).await.unwrap();
        assert_eq!(metadata.id, id);
        assert_eq!(metadata.size, data.len() as i64);
        assert_eq!(metadata.content_type, content_type);

        // Metadata is gone after delete.
        service.delete(&id).await.unwrap();
        let err = service.get_metadata(&id).await.unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    /// Metadata for a never-uploaded blob is not found (table may not even
    /// exist yet).
    #[tokio::test]
    async fn test_get_metadata_nonexistent_blob() {
        let config = BlobStorageConfig::default();
        let db = Arc::new(Database::new());
        let service = BlobStorageService::new(config, db);

        let id = BlobId::new();
        let err = service.get_metadata(&id).await.unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[tokio::test]
    async fn test_store_metadata_creates_table_and_round_trips() {
        let config = BlobStorageConfig::default();
        let db = Arc::new(Database::new());
        let service = BlobStorageService::new(config, db);

        let id = BlobId::new();
        let metadata = BlobMetadata::new(id.clone(), 2048, "application/json".to_string())
            .with_metadata(serde_json::json!({ "custom": "value" }));

        service.store_metadata(&metadata).await.unwrap();

        // Table auto-created on first store.
        assert!(service.db.read().get_table(BLOB_METADATA_TABLE).is_some());

        let retrieved = service.get_metadata(&id).await.unwrap();
        assert_eq!(retrieved.size, 2048);
        assert_eq!(retrieved.content_type, "application/json");
        assert_eq!(retrieved.metadata, Some(serde_json::json!({ "custom": "value" })));
    }

    #[tokio::test]
    async fn test_delete_metadata_is_idempotent() {
        let config = BlobStorageConfig::default();
        let db = Arc::new(Database::new());
        let service = BlobStorageService::new(config, db);

        let id = BlobId::new();
        // No table exists yet — deleting metadata for an unknown blob must
        // not panic or error.
        service.delete_metadata(&id);

        let metadata = BlobMetadata::new(id.clone(), 512, "image/png".to_string());
        service.store_metadata(&metadata).await.unwrap();
        assert!(service.get_metadata(&id).await.is_ok());

        service.delete_metadata(&id);
        assert!(service.get_metadata(&id).await.is_err());

        // Deleting again is a no-op, not an error.
        service.delete_metadata(&id);
    }

    #[cfg(all(feature = "opendal", feature = "storage-memory"))]
    #[tokio::test]
    async fn test_memory_backend_not_found() {
        let config =
            BlobStorageConfig { backend: "memory".to_string(), config: serde_json::json!({}) };
        let db = Arc::new(Database::new());
        let service = BlobStorageService::new(config, db);

        let id = BlobId::new();
        let result = service.get(&id).await;
        assert!(result.is_err());
    }
}
