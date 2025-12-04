# File and Blob Storage

VibeSQL provides integrated file and blob storage with SQL integration, allowing you to store files alongside your relational data and reference them in queries.

## Quick Start

```sql
-- Store a blob reference in your table
CREATE TABLE attachments (
  id INTEGER PRIMARY KEY,
  name VARCHAR(255),
  blob_id TEXT,  -- References stored blob
  uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Query blob metadata
SELECT name, STORAGE_SIZE(blob_id) as size_bytes, STORAGE_URL(blob_id) as url
FROM attachments;
```

## HTTP API

### Upload a File

```bash
curl -X POST http://localhost:8080/api/storage/upload \
  -H "Content-Type: image/png" \
  --data-binary @photo.png
```

**Response:**
```json
{
  "id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "size": 102400,
  "content_type": "image/png",
  "url": "/api/storage/a1b2c3d4-e5f6-7890-abcd-ef1234567890"
}
```

### Download a File

```bash
curl http://localhost:8080/api/storage/a1b2c3d4-e5f6-7890-abcd-ef1234567890 \
  --output downloaded.png
```

### Get Metadata

```bash
curl http://localhost:8080/api/storage/a1b2c3d4-e5f6-7890-abcd-ef1234567890/metadata
```

**Response:**
```json
{
  "id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "size": 102400,
  "content_type": "image/png",
  "created_at": "2024-01-15T10:30:00Z"
}
```

### Delete a File

```bash
curl -X DELETE http://localhost:8080/api/storage/a1b2c3d4-e5f6-7890-abcd-ef1234567890
```

## SQL Functions

### STORAGE_URL(blob_id)

Get a URL for accessing the blob:

```sql
SELECT name, STORAGE_URL(blob_id) as download_url
FROM attachments
WHERE id = 1;
```

**Result:**
```
name          | download_url
--------------+--------------------------------------------------
document.pdf  | /api/storage/a1b2c3d4-e5f6-7890-abcd-ef1234567890
```

### STORAGE_SIZE(blob_id)

Get the size in bytes of a stored blob:

```sql
SELECT name, STORAGE_SIZE(blob_id) as size_bytes
FROM attachments;

-- Calculate total storage used
SELECT SUM(STORAGE_SIZE(blob_id)) as total_bytes
FROM attachments
WHERE user_id = 1;
```

## TypeScript SDK

```typescript
import { VibeSqlClient } from '@vibesql/client';

const db = new VibeSqlClient({ host: 'localhost' });
await db.connect();

// Upload a file
const file = new File(['Hello, World!'], 'hello.txt', { type: 'text/plain' });
const blob = await db.storage.upload(file);
console.log('Uploaded:', blob.id);

// Store reference in database
await db.query(
  'INSERT INTO documents (name, blob_id) VALUES ($1, $2)',
  [file.name, blob.id]
);

// Get download URL
const url = await db.storage.getUrl(blob.id);
console.log('Download URL:', url);

// Delete blob
await db.storage.delete(blob.id);
```

### React Integration

```tsx
import { useStorage } from '@vibesql/client/react';

function FileUpload() {
  const { upload, uploading, error } = useStorage(db);

  const handleUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;

    const blob = await upload(file);

    // Save reference to database
    await db.query(
      'INSERT INTO attachments (name, blob_id, size) VALUES ($1, $2, $3)',
      [file.name, blob.id, blob.size]
    );
  };

  return (
    <div>
      <input type="file" onChange={handleUpload} disabled={uploading} />
      {uploading && <span>Uploading...</span>}
      {error && <span>Error: {error.message}</span>}
    </div>
  );
}
```

## Python SDK

```python
import vibesql

db = vibesql.connect()

# Upload a file
with open('document.pdf', 'rb') as f:
    blob = db.storage.upload(f, content_type='application/pdf')
    print(f'Uploaded: {blob.id}')

# Store reference
cursor = db.cursor()
cursor.execute(
    'INSERT INTO documents (name, blob_id) VALUES (?, ?)',
    ['document.pdf', blob.id]
)
db.commit()

# Get URL
url = db.storage.get_url(blob.id)
print(f'Download URL: {url}')

# Download
data = db.storage.download(blob.id)
with open('downloaded.pdf', 'wb') as f:
    f.write(data)
```

## Storage Patterns

### File Attachments

```sql
-- Schema for file attachments
CREATE TABLE attachments (
  id INTEGER PRIMARY KEY,
  entity_type VARCHAR(50),  -- 'message', 'post', 'comment'
  entity_id INTEGER,
  blob_id TEXT NOT NULL,
  filename VARCHAR(255),
  content_type VARCHAR(100),
  size_bytes BIGINT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Get all attachments for a message
SELECT filename, content_type, size_bytes, STORAGE_URL(blob_id) as url
FROM attachments
WHERE entity_type = 'message' AND entity_id = 123;
```

### User Avatars

```sql
CREATE TABLE users (
  id INTEGER PRIMARY KEY,
  name VARCHAR(100),
  avatar_blob_id TEXT  -- NULL = no avatar
);

-- Get user with avatar URL
SELECT id, name,
       CASE WHEN avatar_blob_id IS NOT NULL
            THEN STORAGE_URL(avatar_blob_id)
            ELSE '/default-avatar.png'
       END as avatar_url
FROM users
WHERE id = 1;
```

### Image Gallery

```sql
CREATE TABLE images (
  id INTEGER PRIMARY KEY,
  album_id INTEGER REFERENCES albums(id),
  original_blob_id TEXT NOT NULL,
  thumbnail_blob_id TEXT,
  title VARCHAR(255),
  width INTEGER,
  height INTEGER,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Get gallery with thumbnails
SELECT title,
       STORAGE_URL(thumbnail_blob_id) as thumbnail,
       STORAGE_URL(original_blob_id) as full_size
FROM images
WHERE album_id = 1
ORDER BY created_at DESC;
```

## Configuration

### Local Filesystem (Default)

```toml
[storage]
backend = "fs"
root = "/var/vibesql/storage"
```

### S3 / MinIO

```toml
[storage]
backend = "s3"
bucket = "my-bucket"
region = "us-east-1"
endpoint = "https://s3.amazonaws.com"  # Or MinIO URL
access_key_id = "AKIAIOSFODNN7EXAMPLE"
secret_access_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
```

### Google Cloud Storage

```toml
[storage]
backend = "gcs"
bucket = "my-bucket"
project_id = "my-project"
# Uses Application Default Credentials
```

### Azure Blob Storage

```toml
[storage]
backend = "azblob"
container = "my-container"
account_name = "mystorageaccount"
account_key = "..."
```

## System Table

Blob metadata is stored in the `vibesql_storage` system table:

```sql
-- Query blob metadata directly
SELECT * FROM vibesql_storage WHERE id = 'a1b2c3d4...';

-- Find large files
SELECT id, size, content_type, created_at
FROM vibesql_storage
WHERE size > 10485760  -- > 10MB
ORDER BY size DESC;

-- Storage usage by content type
SELECT content_type, COUNT(*) as count, SUM(size) as total_bytes
FROM vibesql_storage
GROUP BY content_type
ORDER BY total_bytes DESC;
```

## Best Practices

### Use Transactions

When storing a file and its database reference, use transactions:

```typescript
await db.transaction(async (tx) => {
  // Upload file first
  const blob = await db.storage.upload(file);

  // Then store reference
  await tx.query(
    'INSERT INTO attachments (blob_id, name) VALUES ($1, $2)',
    [blob.id, file.name]
  );
});
```

### Content Type Validation

Validate content types before accepting uploads:

```typescript
const ALLOWED_TYPES = ['image/jpeg', 'image/png', 'application/pdf'];

if (!ALLOWED_TYPES.includes(file.type)) {
  throw new Error('File type not allowed');
}
```

### Size Limits

Configure upload size limits:

```toml
[storage]
max_upload_size = 52428800  # 50MB
```

### Cleanup Orphaned Blobs

Periodically clean up blobs not referenced by any table:

```sql
-- Find orphaned blobs (not referenced anywhere)
DELETE FROM vibesql_storage
WHERE id NOT IN (SELECT blob_id FROM attachments)
  AND id NOT IN (SELECT avatar_blob_id FROM users WHERE avatar_blob_id IS NOT NULL)
  AND created_at < NOW() - INTERVAL '7 days';
```

## Limitations

- Maximum file size: Configurable, default 100MB
- Content types: Any (no server-side validation by default)
- Blob IDs are UUIDs, immutable once created
- Blobs are stored separately from the database file

## See Also

- [HTTP API](http-api.md) - Full HTTP endpoint reference
- [TypeScript SDK](../packages/vibesql-client-ts/README.md) - TypeScript client
- [Python Bindings](PYTHON_BINDINGS.md) - Python integration
