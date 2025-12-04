# HTTP REST and GraphQL API

VibeSQL provides a comprehensive HTTP API for accessing your database without needing the PostgreSQL wire protocol. This includes REST endpoints, GraphQL queries, and real-time subscriptions via Server-Sent Events (SSE).

## Quick Start

```bash
# Start the server with HTTP enabled
cargo run --release --bin vibesql -- --http-port 8080

# Execute a query
curl -X POST http://localhost:8080/api/query \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT * FROM users LIMIT 10"}'

# Use auto-generated CRUD endpoints
curl http://localhost:8080/api/tables/users/rows?limit=10
```

## Endpoints Overview

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/api/query` | POST | Execute arbitrary SQL |
| `/api/subscribe` | GET | SSE stream for real-time updates |
| `/api/tables` | GET | List all tables |
| `/api/tables/:table` | GET | Get table schema |
| `/api/tables/:table/rows` | GET | List rows (with filtering) |
| `/api/tables/:table/rows` | POST | Insert row |
| `/api/tables/:table/rows/:id` | GET | Get single row |
| `/api/tables/:table/rows/:id` | PUT | Full update |
| `/api/tables/:table/rows/:id` | PATCH | Partial update |
| `/api/tables/:table/rows/:id` | DELETE | Delete row |
| `/api/graphql` | POST | GraphQL endpoint |
| `/api/storage/upload` | POST | Upload blob |
| `/api/storage/:id` | GET | Download blob |
| `/api/storage/:id` | DELETE | Delete blob |
| `/api/storage/:id/metadata` | GET | Get blob metadata |

## SQL Query Endpoint

Execute arbitrary SQL queries via HTTP:

```bash
# Simple query
curl -X POST http://localhost:8080/api/query \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT id, name FROM users WHERE active = true"}'

# With parameters
curl -X POST http://localhost:8080/api/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "SELECT * FROM users WHERE age > $1 AND status = $2",
    "params": [21, "active"]
  }'
```

**Response:**
```json
{
  "columns": ["id", "name"],
  "rows": [
    [1, "Alice"],
    [2, "Bob"]
  ],
  "row_count": 2
}
```

## Auto-Generated CRUD Endpoints

Every table automatically gets RESTful CRUD endpoints without any configuration.

### List Rows

```bash
# Basic listing
GET /api/tables/users/rows

# With pagination
GET /api/tables/users/rows?limit=10&offset=20

# Select specific columns
GET /api/tables/users/rows?select=id,name,email

# Order results
GET /api/tables/users/rows?order=created_at.desc

# Filter by column value
GET /api/tables/users/rows?status=eq.active
GET /api/tables/users/rows?age=gt.21
GET /api/tables/users/rows?name=like.%smith%
```

### Filter Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `eq` | Equal to | `status=eq.active` |
| `neq` | Not equal to | `status=neq.deleted` |
| `gt` | Greater than | `age=gt.21` |
| `gte` | Greater than or equal | `age=gte.21` |
| `lt` | Less than | `price=lt.100` |
| `lte` | Less than or equal | `price=lte.100` |
| `like` | SQL LIKE pattern | `name=like.%smith%` |
| `ilike` | Case-insensitive LIKE | `email=ilike.%@GMAIL.COM` |
| `in` | In list | `status=in.(active,pending)` |
| `is` | IS NULL check | `deleted_at=is.null` |

### Create Row

```bash
curl -X POST http://localhost:8080/api/tables/users/rows \
  -H "Content-Type: application/json" \
  -d '{"name": "Alice", "email": "alice@example.com", "age": 30}'
```

**Response:**
```json
{
  "data": {"id": 1, "name": "Alice", "email": "alice@example.com", "age": 30},
  "affected_rows": 1
}
```

### Get Single Row

```bash
GET /api/tables/users/rows/1
```

### Update Row (Full)

Replace all columns (missing columns set to NULL):

```bash
curl -X PUT http://localhost:8080/api/tables/users/rows/1 \
  -H "Content-Type: application/json" \
  -d '{"name": "Alice Smith", "email": "alice.smith@example.com", "age": 31}'
```

### Update Row (Partial)

Update only specified columns:

```bash
curl -X PATCH http://localhost:8080/api/tables/users/rows/1 \
  -H "Content-Type: application/json" \
  -d '{"age": 31}'
```

### Delete Row

```bash
curl -X DELETE http://localhost:8080/api/tables/users/rows/1
```

## GraphQL Endpoint

VibeSQL provides a GraphQL-like interface that auto-generates a schema from your database.

### Query

```graphql
POST /api/graphql

{
  "query": "query { users(where: { active: { eq: true } }, limit: 10) { id name email } }"
}
```

### With Variables

```json
{
  "query": "query GetUser($id: Int!) { users(where: { id: { eq: $id } }) { id name email } }",
  "variables": { "id": 1 }
}
```

### WHERE Clause Operators

The GraphQL API supports structured filtering:

**Comparison Operators:**
- `eq` - Equal to
- `ne` - Not equal to
- `gt` - Greater than
- `gte` - Greater than or equal
- `lt` - Less than
- `lte` - Less than or equal

**String Operators:**
- `like` - SQL LIKE pattern
- `ilike` - Case-insensitive LIKE
- `contains` - Contains substring
- `startsWith` - Starts with prefix
- `endsWith` - Ends with suffix

**List Operators:**
- `in` - Value in list
- `notIn` - Value not in list

**Null Operators:**
- `isNull` - Check for NULL (true/false)

**Logical Combinators:**
- `AND` - Array of conditions combined with AND
- `OR` - Array of conditions combined with OR
- `NOT` - Negate a condition

### Complex Query Example

```graphql
query {
  users(where: {
    age: { gte: 18 },
    OR: [
      { name: { contains: "smith" } },
      { email: { endsWith: "@company.com" } }
    ]
  }, limit: 50, order: { created_at: DESC }) {
    id
    name
    email
    posts {
      id
      title
    }
  }
}
```

### Schema Introspection

The GraphQL endpoint supports standard introspection queries:

```graphql
{
  __schema {
    types {
      name
      fields {
        name
        type { name }
      }
    }
  }
}
```

### Mutations

```graphql
mutation {
  insert_users(objects: [{ name: "Bob", email: "bob@example.com" }]) {
    returning {
      id
      name
    }
  }
}

mutation {
  update_users(where: { id: { eq: 1 } }, _set: { name: "Robert" }) {
    affected_rows
  }
}

mutation {
  delete_users(where: { id: { eq: 1 } }) {
    affected_rows
  }
}
```

## Real-Time Subscriptions (SSE)

Subscribe to query results via Server-Sent Events:

```bash
curl -N http://localhost:8080/api/subscribe?query=SELECT%20*%20FROM%20messages%20WHERE%20channel_id%20%3D%201
```

**Events:**
```
event: data
data: {"rows": [{"id": 1, "text": "Hello"}]}

event: delta
data: {"type": "insert", "row": {"id": 2, "text": "World"}}

event: delta
data: {"type": "update", "old": {"id": 1, "text": "Hello"}, "new": {"id": 1, "text": "Hi"}}

event: delta
data: {"type": "delete", "row": {"id": 1, "text": "Hi"}}
```

### JavaScript Client

```javascript
const eventSource = new EventSource(
  'http://localhost:8080/api/subscribe?query=' +
  encodeURIComponent('SELECT * FROM messages WHERE channel_id = 1')
);

eventSource.addEventListener('data', (e) => {
  const data = JSON.parse(e.data);
  console.log('Full data:', data.rows);
});

eventSource.addEventListener('delta', (e) => {
  const delta = JSON.parse(e.data);
  if (delta.type === 'insert') {
    console.log('New row:', delta.row);
  } else if (delta.type === 'delete') {
    console.log('Deleted row:', delta.row);
  }
});
```

## Table Schema Endpoints

### List All Tables

```bash
GET /api/tables
```

**Response:**
```json
{
  "tables": ["users", "posts", "comments"]
}
```

### Get Table Info

```bash
GET /api/tables/users
```

**Response:**
```json
{
  "name": "users",
  "columns": [
    {"name": "id", "type": "INTEGER", "nullable": false, "primary_key": true},
    {"name": "name", "type": "VARCHAR(100)", "nullable": false},
    {"name": "email", "type": "VARCHAR(255)", "nullable": false},
    {"name": "created_at", "type": "TIMESTAMP", "nullable": true}
  ],
  "primary_key": ["id"],
  "foreign_keys": []
}
```

## Error Handling

All endpoints return consistent error responses:

```json
{
  "error": {
    "code": "QUERY_ERROR",
    "message": "Column 'nonexistent' does not exist in table 'users'"
  }
}
```

**HTTP Status Codes:**
- `200` - Success
- `201` - Created (POST)
- `400` - Bad request (invalid query/parameters)
- `404` - Not found (table or row doesn't exist)
- `500` - Internal server error

## Configuration

Configure HTTP API settings in your server config:

```toml
[http]
# HTTP port (default: 8080)
port = 8080

# Enable CORS
cors_enabled = true
cors_origins = ["http://localhost:3000"]

# Rate limiting
rate_limit_requests = 100
rate_limit_window_secs = 60
```

## Security

For production deployments:

1. **Use HTTPS** - Deploy behind a reverse proxy with TLS
2. **Authentication** - Configure API keys or JWT authentication
3. **Rate Limiting** - Enable rate limiting to prevent abuse
4. **Input Validation** - All inputs are parameterized to prevent SQL injection

## See Also

- [TypeScript SDK](../packages/vibesql-client-ts/README.md) - Native TypeScript client
- [Drizzle ORM](../packages/vibesql-drizzle/README.md) - Type-safe queries with Drizzle
- [Scheduled Functions](scheduled-functions.md) - Background tasks and cron jobs
- [File Storage](file-storage.md) - Blob storage API details
