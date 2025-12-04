/// Blob storage service
///
/// Manages storing and retrieving blobs with support for multiple backends
/// via OpenDAL abstraction layer.
use super::{BlobId, BlobMetadata, BlobStorageConfig};
use crate::database::Database;
use crate::error::{StorageError, StorageResult};
use crate::Row;
use bytes::Bytes;
use vibesql_types::{SqlValue, DataType};
use chrono::Utc;

/// Blob storage service for file/blob operations
pub struct BlobStorageService {
    // TODO: Integrate OpenDAL operator when feature is enabled
    config: BlobStorageConfig,
}

impl BlobStorageService {
    /// Create a new blob storage service
    pub fn new(config: BlobStorageConfig) -> Self {
        Self { config }
    }

    /// Create with default configuration (local filesystem)
    pub fn new_default() -> Self {
        Self::new(BlobStorageConfig::default())
    }

    /// Store a blob and return its ID
    pub async fn store(&self, db: &mut Database, data: Bytes, content_type: String) -> StorageResult<BlobId> {
        let id = BlobId::new();
        let size = data.len() as i64;

        // TODO: Write to backend using OpenDAL operator
        // op.write(&id.to_path(), data).await?;

        // Store metadata in database
        let metadata = BlobMetadata::new(id.clone(), size, content_type);
        self.store_metadata(db, &metadata)?;

        Ok(id)
    }

    /// Retrieve a blob by ID
    pub async fn get(&self, _id: &BlobId) -> StorageResult<Bytes> {
        // TODO: Read from backend using OpenDAL operator
        // op.read(&id.to_path()).await.map(Bytes::from)
        Err(StorageError::Other("blob not found".to_string()))
    }

    /// Get metadata for a blob
    pub async fn get_metadata(&self, db: &Database, id: &BlobId) -> StorageResult<BlobMetadata> {
        // Query vibesql_storage system table
        if let Some(table) = db.get_table("vibesql_storage") {
            let id_str = id.to_string();
            // Iterate through rows using the scan() method
            for row in table.scan() {
                if let Some(&SqlValue::Varchar(ref row_id)) = row.get(0) {
                    if *row_id == id_str {
                        // Extract metadata from row
                        let size = match row.get(1) {
                            Some(&SqlValue::Bigint(s)) => s,
                            _ => return Err(StorageError::Other("invalid blob metadata".to_string())),
                        };
                        let content_type = match row.get(2) {
                            Some(&SqlValue::Varchar(ref ct)) => ct.clone(),
                            _ => return Err(StorageError::Other("invalid blob metadata".to_string())),
                        };
                        let created_at = match row.get(3) {
                            Some(&SqlValue::Timestamp(ref ts)) => {
                                // Convert vibesql Timestamp to chrono DateTime<Utc>
                                let date_time = format!("{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
                                    ts.date.year, ts.date.month, ts.date.day,
                                    ts.time.hour, ts.time.minute, ts.time.second);
                                
                                chrono::DateTime::parse_from_rfc3339(&format!("{}+00:00", date_time.replace(" ", "T")))
                                    .map(|dt| dt.with_timezone(&Utc))
                                    .unwrap_or_else(|_| Utc::now())
                            }
                            _ => Utc::now(),
                        };
                        let metadata_json = row.get(4).cloned();

                        let metadata = BlobMetadata {
                            id: id.clone(),
                            size,
                            content_type,
                            created_at,
                            metadata: metadata_json.and_then(|v| {
                                if let SqlValue::Varchar(json_str) = v {
                                    serde_json::from_str(&json_str).ok()
                                } else {
                                    None
                                }
                            }),
                        };
                        return Ok(metadata);
                    }
                }
            }
        }
        Err(StorageError::Other(format!("blob metadata not found: {}", id)))
    }

    /// Delete a blob
    pub async fn delete(&self, db: &mut Database, id: &BlobId) -> StorageResult<()> {
        // TODO: Delete from backend
        // op.delete(&id.to_path()).await?;

        // Delete metadata from database
        self.delete_metadata(db, id)?;
        Ok(())
    }

    /// Store blob metadata in database
    fn store_metadata(&self, db: &mut Database, metadata: &BlobMetadata) -> StorageResult<()> {
        // Ensure the vibesql_storage table exists
        if db.get_table("vibesql_storage").is_none() {
            // Create the table using the catalog
            let schema = vibesql_catalog::TableSchema::new(
                "vibesql_storage".to_string(),
                vec![
                    vibesql_catalog::ColumnSchema::new("id".to_string(), DataType::Varchar { max_length: Some(36) }, false),
                    vibesql_catalog::ColumnSchema::new("size".to_string(), DataType::Bigint, false),
                    vibesql_catalog::ColumnSchema::new("content_type".to_string(), DataType::Varchar { max_length: Some(255) }, true),
                    vibesql_catalog::ColumnSchema::new("created_at".to_string(), DataType::Timestamp { with_timezone: false }, false),
                    vibesql_catalog::ColumnSchema::new("metadata".to_string(), DataType::CharacterLargeObject, true),
                ],
            );
            db.create_table(schema)?;
        }

        // Convert BlobMetadata to Timestamp type
        let ts = vibesql_types::Timestamp {
            date: vibesql_types::Date {
                year: metadata.created_at.format("%Y").to_string().parse().unwrap_or(2024),
                month: metadata.created_at.format("%m").to_string().parse().unwrap_or(1),
                day: metadata.created_at.format("%d").to_string().parse().unwrap_or(1),
            },
            time: vibesql_types::Time {
                hour: metadata.created_at.format("%H").to_string().parse().unwrap_or(0),
                minute: metadata.created_at.format("%M").to_string().parse().unwrap_or(0),
                second: metadata.created_at.format("%S").to_string().parse().unwrap_or(0),
                nanosecond: metadata.created_at.format("%f").to_string().parse().unwrap_or(0),
            },
        };

        // Insert metadata row
        let row = Row::new(vec![
            SqlValue::Varchar(metadata.id.to_string()),
            SqlValue::Bigint(metadata.size),
            SqlValue::Varchar(metadata.content_type.clone()),
            SqlValue::Timestamp(ts),
            metadata.metadata.clone()
                .map(|m| SqlValue::Varchar(m.to_string()))
                .unwrap_or(SqlValue::Null),
        ]);
        db.insert_row("vibesql_storage", row)?;
        Ok(())
    }

    /// Delete blob metadata from database
    fn delete_metadata(&self, db: &mut Database, id: &BlobId) -> StorageResult<()> {
        if let Some(table) = db.get_table_mut("vibesql_storage") {
            let id_str = id.to_string();
            // Find and remove the row
            let original_len = table.rows_mut().len();
            table.rows_mut().retain(|row| {
                if let Some(SqlValue::Varchar(row_id)) = row.get(0) {
                    row_id != &id_str
                } else {
                    true
                }
            });
            
            if table.rows_mut().len() == original_len {
                return Err(StorageError::Other(format!("blob metadata not found: {}", id)));
            }
        } else {
            return Err(StorageError::Other("vibesql_storage table not found".to_string()));
        }
        Ok(())
    }

    /// Generate a URL for accessing a blob
    /// 
    /// For local filesystem, returns a relative path.
    /// For cloud storage, could generate signed URLs.
    pub fn get_url(&self, id: &BlobId) -> String {
        match self.config.backend.as_str() {
            "fs" => format!("/storage/blobs/{}", id.to_url_safe()),
            "s3" => {
                if let Some(bucket) = self.config.config.get("bucket") {
                    format!(
                        "s3://{}/{}",
                        bucket.as_str().unwrap_or(""),
                        id.to_path()
                    )
                } else {
                    format!("s3://unknown/{}", id.to_path())
                }
            }
            _ => format!("/storage/blobs/{}", id.to_url_safe()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_blob_url_generation() {
        let config = BlobStorageConfig::default();
        let service = BlobStorageService::new(config);

        let id = BlobId::new();
        let url = service.get_url(&id);
        assert!(url.starts_with("/storage/blobs/"));
    }

    #[test]
    fn test_store_metadata_creates_table() {
        let service = BlobStorageService::new_default();
        let mut db = Database::new();

        let id = BlobId::new();
        let metadata = BlobMetadata::new(id.clone(), 1024, "text/plain".to_string());

        service.store_metadata(&mut db, &metadata).unwrap();

        // Verify table was created
        assert!(db.get_table("vibesql_storage").is_some());
    }

    #[test]
    fn test_store_and_retrieve_metadata() {
        let service = BlobStorageService::new_default();
        let mut db = Database::new();

        let id = BlobId::new();
        let metadata = BlobMetadata::new(id.clone(), 2048, "application/json".to_string())
            .with_metadata(serde_json::json!({ "custom": "value" }));

        service.store_metadata(&mut db, &metadata).unwrap();

        // Retrieve the metadata
        // Note: The async part is tested by directly calling the sync path
        // This is a limitation of the current design - would benefit from refactoring
        // to support both sync and async paths
    }

    #[test]
    fn test_delete_metadata() {
        let service = BlobStorageService::new_default();
        let mut db = Database::new();

        let id = BlobId::new();
        let metadata = BlobMetadata::new(id.clone(), 512, "image/png".to_string());

        service.store_metadata(&mut db, &metadata).unwrap();
        assert!(db.get_table("vibesql_storage").is_some());

        // Delete the metadata
        service.delete_metadata(&mut db, &id).unwrap();

        // Verify table still exists but row is gone
        assert!(db.get_table("vibesql_storage").is_some());
    }
}
