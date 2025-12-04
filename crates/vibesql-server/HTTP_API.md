# VibeSQL HTTP REST API

The VibeSQL server now supports HTTP REST API endpoints in addition to the PostgreSQL wire protocol. This enables web-based access to the database without requiring a PostgreSQL client.

## Configuration

Enable the HTTP server in your `vibesql-server.toml`:

```toml
[http]
enabled = true
host = "0.0.0.0"
port = 8080
```

The HTTP server runs alongside the PostgreSQL wire protocol server on a separate port.

## Endpoints

### Health Check

```
GET /health
```

Returns server status and version.

**Response:**
```json
{
  "status": "ok",
  "version": "0.1.0"
}
```

### Execute Query

```
POST /api/query
Content-Type: application/json
```

Execute a SQL query with optional parameters and pagination.

#### Pagination

You can optionally include pagination parameters to limit and offset results:

- `limit` (number): Maximum number of rows to return
- `offset` (number): Number of rows to skip (zero-indexed)

**Request (with pagination):**
```json
{
  "sql": "SELECT * FROM users WHERE status = $1",
  "params": ["active"],
  "limit": 10,
  "offset": 20
}
```

**Response (SELECT without pagination):**
```json
{
  "columns": ["id", "name", "email"],
  "rows": [
    [123, "Alice", "alice@example.com"],
    [124, "Bob", "bob@example.com"]
  ],
  "row_count": 2
}
```

**Response (SELECT with pagination):**
```json
{
  "columns": ["id", "name", "email"],
  "rows": [
    [23, "User 23", "user23@example.com"],
    [24, "User 24", "user24@example.com"]
  ],
  "row_count": 2,
  "total_count": 100,
  "offset": 20,
  "limit": 10
}
```

**Response (INSERT/UPDATE/DELETE):**
```json
{
  "rows_affected": 5
}
```

#### Pagination Examples

Get first 10 users:
```json
{
  "sql": "SELECT * FROM users",
  "limit": 10
}
```

Get second page (users 11-20):
```json
{
  "sql": "SELECT * FROM users",
  "limit": 10,
  "offset": 10
}
```

Get all matching records with count:
```json
{
  "sql": "SELECT * FROM users WHERE active = true"
}
```
The response will include `total_count` with the total matching rows.

### List Tables

```
GET /api/tables
```

Returns all table names in the database.

**Response:**
```json
{
  "tables": ["users", "posts", "comments"],
  "count": 3
}
```

### Get Table Information

```
GET /api/tables/:table_name
```

Returns schema information for a specific table.

**Response:**
```json
{
  "name": "users",
  "columns": [
    {
      "name": "*",
      "data_type": "unknown",
      "nullable": true,
      "primary_key": false
    }
  ]
}
```

## Examples

### Using cURL

```bash
# Health check
curl http://localhost:8080/health

# Execute a query
curl -X POST http://localhost:8080/api/query \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT * FROM users LIMIT 10"
  }'

# List tables
curl http://localhost:8080/api/tables
```

### Using JavaScript/Fetch

```javascript
// Execute a query
const response = await fetch('http://localhost:8080/api/query', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    sql: 'SELECT * FROM users WHERE age > $1',
    params: [18]
  })
});

const result = await response.json();
console.log(result.rows);
```

## Data Type Conversions

SQL values are converted to JSON types as follows:

| SQL Type | JSON Type | Notes |
|----------|-----------|-------|
| NULL | null | |
| BOOLEAN | boolean | |
| INTEGER, SMALLINT, BIGINT | number | |
| UNSIGNED | number | |
| NUMERIC, FLOAT, DOUBLE, REAL | number | NaN/Infinity become null |
| CHARACTER, VARCHAR | string | |
| DATE, TIME, TIMESTAMP | string | Formatted as ISO 8601 |
| INTERVAL | null | Not yet supported |

JSON parameter values are converted to SQL types:

- `null` → NULL
- `boolean` → BOOLEAN
- `number` (integer) → INTEGER
- `number` (float) → NUMERIC
- `string` → VARCHAR

## Error Responses

HTTP errors return JSON with error details:

```json
{
  "error": "Query execution failed: syntax error",
  "code": null
}
```

Status codes:
- `200 OK` - Successful query execution
- `201 Created` - INSERT successful
- `400 Bad Request` - Invalid query or parameters
- `404 Not Found` - Table not found
- `500 Internal Server Error` - Server error

## Performance Notes

- Each HTTP request creates a new session and database instance
- Queries are executed independently without transaction support (Phase 1)
- Connection pooling will be added in future phases
- Prepared statements are cached per-session

## Future Phases

### Phase 2: Real-time HTTP
- Server-Sent Events (SSE) for subscriptions
- WebSocket support for persistent connections
- Connection management and pooling

### Phase 3: GraphQL
- Auto-generated GraphQL schema from SQL tables
- Query/Mutation/Subscription support
- GraphQL playground

### Phase 4: Webhooks
- Webhook registration and management
- Async delivery with retries
- HMAC signature verification

## Implementation Status

Current implementation covers **Phase 1: Basic HTTP REST** with:
- [x] REST endpoint for SQL queries
- [x] Parameter binding
- [x] Query result formatting (SELECT)
- [x] Mutation responses (INSERT/UPDATE/DELETE)
- [x] Table listing and schema introspection
- [x] Error handling and JSON responses
- [ ] Authentication (API key, JWT)
- [ ] Rate limiting
- [ ] OpenAPI/Swagger documentation

See issue #3461 for roadmap and future enhancements.
