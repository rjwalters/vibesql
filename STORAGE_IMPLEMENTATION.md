# File and Blob Storage Implementation

## Overview

This implementation adds file/blob storage capabilities to VibeSql with SQL integration, using the OpenDAL library for abstraction over multiple storage backends.

## Phase 1: Core Infrastructure (Completed)

### 1.1 Blob Storage Module (`crates/vibesql-storage/src/blob/`)

#### `BlobId` (`blob/id.rs`)
- Unique UUID-based identifier for stored blobs
- Supports conversion to/from strings
- Generates hierarchical paths for storage backends (e.g., `ab/c1/2b3c...`)
- Implements `Display`, `FromStr`, `Serialize`, `Deserialize`

#### `BlobMetadata` (`blob/mod.rs`)
- Stores metadata about blobs:
  - ID (UUID)
  - Size in bytes
  - Content type (MIME type)
  - Creation timestamp
  - Optional custom metadata (JSON)
- Used for tracking blob information in the database

#### `BlobStorageService` (`blob/service.rs`)
- Core service for blob operations
- Methods:
  - `store()` - Store blob and return ID
  - `get()` - Retrieve blob by ID
  - `delete()` - Delete blob
  - `get_metadata()` - Get blob metadata
  - `get_url()` - Generate access URL
- Supports multiple backends via OpenDAL (currently stubbed):
  - Local filesystem (default)
  - S3/S3-compatible
  - GCS
  - Azure Blob Storage
  - HDFS
  - etc. (40+ backends supported by OpenDAL)

### 1.2 SQL Functions (`crates/vibesql-executor/src/evaluator/functions/storage.rs`)

#### `STORAGE_URL(blob_id TEXT) -> TEXT`
Generate a URL for accessing a stored blob
```sql
SELECT STORAGE_URL(blob_id) FROM attachments;
```

#### `STORAGE_SIZE(blob_id TEXT) -> BIGINT`
Get the size in bytes of a stored blob
```sql
SELECT STORAGE_SIZE(blob_id) FROM attachments;
```

### 1.3 Dependencies

Added to `crates/vibesql-storage/Cargo.toml`:
- `uuid` - For generating and managing blob IDs
- `bytes` - For efficient binary data handling
- `tokio` - For async operations (optional, behind feature flag)
- `opendal` - For multi-backend storage abstraction (optional, behind feature flag)

## Phase 2: System Table (#3482)

### vibesql_storage Table
System table for storing blob metadata:
```sql
CREATE TABLE vibesql_storage (
  id TEXT PRIMARY KEY,
  size BIGINT,
  content_type TEXT,
  created_at TIMESTAMP,
  metadata JSON
);
```

To be implemented:
- Auto-created during database initialization
- Insert metadata when blob is stored
- Cleanup when blob is deleted
- Query for size/metadata lookups

## Phase 3: HTTP API Endpoints (#3483)

To be implemented:
- `POST /api/storage/upload` - Upload blob, returns blob ID
- `GET /api/storage/{blob_id}` - Download blob by ID
- Content-Type and size headers
- Support for multipart uploads

## Phase 4: TypeScript SDK (#3484)

To be implemented:
```typescript
// Upload
const storageId = await db.storage.upload(file);

// Reference in DB
await db.query(
  'INSERT INTO attachments (storage_id, filename) VALUES ($1, $2)',
  [storageId, file.name]
);

// Get URL
const url = await db.storage.getUrl(storageId);
```

## Phase 5: Full OpenDAL Integration (#3485)

To be implemented:
- Enable OpenDAL feature flag
- Implement backend selection based on configuration
- Support for:
  - Local filesystem with proper isolation
  - S3 with configurable endpoint/bucket
  - Multi-cloud support
  - Signed URLs for cloud storage
  - Object metadata preservation

## Architecture

### Module Structure
```
vibesql-storage/
  src/blob/
    mod.rs          - Public types and configuration
    id.rs           - BlobId implementation
    service.rs      - BlobStorageService implementation

vibesql-executor/
  src/evaluator/functions/
    storage.rs      - SQL function implementations
```

### Data Flow

1. **Upload**:
   ```
   User -> API Endpoint -> BlobStorageService.store() -> Backend
                             |
                             v
                          vibesql_storage table
   ```

2. **Download**:
   ```
   User -> SQL Query -> vibesql_storage table -> BlobStorageService.get() -> Backend
                        STORAGE_URL() function -> generates URL
   ```

3. **Metadata**:
   ```
   SQL Query -> vibesql_storage table
   STORAGE_SIZE() function -> queries metadata table
   ```

## Configuration

Default configuration (local filesystem):
```toml
[storage]
backend = "fs"
root = "/var/vibesql/storage"
```

To be supported:
```toml
# S3/MinIO
[storage]
backend = "s3"
bucket = "my-bucket"
endpoint = "https://s3.amazonaws.com"
access_key_id = "XXXX"
secret_access_key = "XXXX"

# GCS
[storage]
backend = "gcs"
bucket = "my-bucket"
project_id = "my-project"
```

## Testing

### Current Tests
- `blob::id::tests` - BlobId creation, parsing, path generation
- `blob::tests` - BlobMetadata creation
- `blob::service::tests` - URL generation
- `evaluator::functions::storage::tests` - SQL function behavior

### Future Tests
- Integration tests for actual file operations
- Multi-backend integration tests
- Concurrent access tests
- Storage quota tests
- Garbage collection tests

## Security Considerations

To be implemented:
1. **Access Control** - Only authorized users can access blobs
2. **Encryption** - Support for encrypted storage
3. **Signed URLs** - Time-limited access URLs for cloud storage
4. **Isolation** - Blobs isolated from other data
5. **Quota** - Per-user/table storage limits
6. **Validation** - Content-type validation, file scanning

## Future Enhancements

1. **Chunk/Stream Upload** - Support for large files
2. **Compression** - Automatic compression for storage
3. **Versioning** - Keep blob versions with MVCC
4. **Replication** - Replicate blobs across backends
5. **CDN Integration** - Serve blobs through CDN
6. **Thumbnails** - Auto-generate thumbnails for images
7. **Virus Scanning** - Scan uploaded files
8. **Audit Trail** - Log all storage operations

## References

- [Apache OpenDAL](https://opendal.apache.org/)
- [object_store](https://docs.rs/object_store)
- Related Issue: #3461 - HTTP API (upload/download endpoints)
