# VibeSQL GraphQL API Documentation

## Overview

The VibeSQL GraphQL API provides an alternative query interface alongside the REST API. While not a full GraphQL implementation, it provides a lightweight GraphQL-like syntax for querying and mutating data in the database.

**Endpoint**: `POST /api/graphql`

## Request Format

All GraphQL requests are POST requests with a JSON body containing:

```json
{
  "query": "GraphQL query string",
  "variables": { /* optional variables */ },
  "operationName": "optional operation name"
}
```

## Response Format

All responses follow the GraphQL response format:

```json
{
  "data": { /* result data */ },
  "errors": [ /* optional error array */ ]
}
```

## Queries

### Simple SELECT Query

Query all records from a table:

```json
{
  "query": "{ users { id name email } }"
}
```

Response:

```json
{
  "data": {
    "data": [
      { "id": 1, "name": "Alice", "email": "alice@example.com" },
      { "id": 2, "name": "Bob", "email": "bob@example.com" }
    ]
  }
}
```

### Query with WHERE Clause

Filter records with a WHERE clause:

```json
{
  "query": "{ users(where: \"email = 'alice@example.com'\") { id name email } }"
}
```

### Query All Columns

Use `*` to select all columns:

```json
{
  "query": "{ users { * } }"
}
```

This returns all columns from the users table.

### Health Check Query

Check API health and version:

```json
{
  "query": "query { health { status version } }"
}
```

Response:

```json
{
  "data": {
    "status": "ok",
    "version": "0.1.1"
  }
}
```

## Nested Queries (Relationship Resolution)

VibeSQL automatically detects foreign key relationships between tables and allows nested queries to traverse these relationships. This avoids N+1 query problems by using batched queries.

### One-to-Many Relationships

Query a parent table with its related child records:

```json
{
  "query": "{ users { id name posts { id title } } }"
}
```

Response (each user includes their posts as a nested array):

```json
{
  "data": {
    "data": [
      {
        "id": 1,
        "name": "Alice",
        "posts": [
          { "id": 1, "title": "First Post" },
          { "id": 2, "title": "Second Post" }
        ]
      },
      {
        "id": 2,
        "name": "Bob",
        "posts": []
      }
    ]
  }
}
```

### Many-to-One Relationships

Query a child table with its related parent record:

```json
{
  "query": "{ posts { id title user { id name } } }"
}
```

Response (each post includes its author as a nested object):

```json
{
  "data": {
    "data": [
      {
        "id": 1,
        "title": "First Post",
        "user": { "id": 1, "name": "Alice" }
      },
      {
        "id": 2,
        "title": "Second Post",
        "user": { "id": 1, "name": "Alice" }
      }
    ]
  }
}
```

### Deep Nesting (3+ Levels)

Nested queries can be arbitrarily deep:

```json
{
  "query": "{ users { id name posts { id title comments { id body } } } }"
}
```

### Pagination on Nested Queries

Apply limits to nested queries using the `limit` and `offset` parameters:

```json
{
  "query": "{ users { id name posts(limit: 5, offset: 0) { id title } } }"
}
```

### How It Works

1. **Foreign Key Detection**: VibeSQL reads the schema to identify foreign key relationships
2. **Relationship Direction**:
   - **One-to-Many**: Parent table (e.g., `users`) → child table has FK (e.g., `posts.user_id`)
   - **Many-to-One**: Child table (e.g., `posts`) → parent table is referenced
3. **Batched Queries**: Nested data is fetched using `WHERE IN (...)` clauses to avoid N+1 problems
4. **Result Structuring**: Results are grouped and attached to parent records

### Schema Requirements

For relationship resolution to work, your schema must have defined foreign key constraints:

```sql
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name VARCHAR(255)
);

CREATE TABLE posts (
    id INTEGER PRIMARY KEY,
    title VARCHAR(255),
    user_id INTEGER REFERENCES users(id)
);
```

## Mutations

### INSERT Mutation

Insert a new record:

```json
{
  "query": "mutation { insert(table: \"users\", values: {\"name\": \"Charlie\", \"email\": \"charlie@example.com\"}) }"
}
```

Response:

```json
{
  "data": {
    "rowsAffected": 1
  }
}
```

### UPDATE Mutation

Update existing records:

```json
{
  "query": "mutation { update(table: \"users\", values: {\"email\": \"newemail@example.com\"}, where: \"id = 1\") }"
}
```

Response:

```json
{
  "data": {
    "rowsAffected": 1
  }
}
```

### DELETE Mutation

Delete records (requires WHERE clause for safety):

```json
{
  "query": "mutation { delete(table: \"users\", where: \"id = 3\") }"
}
```

Response:

```json
{
  "data": {
    "rowsAffected": 1
  }
}
```

## Error Handling

If an error occurs, the response will include an `errors` array:

```json
{
  "data": null,
  "errors": [
    {
      "message": "Table 'invalid_table' not found"
    }
  ]
}
```

Common errors include:

- **Parse errors**: Invalid GraphQL syntax
- **Conversion errors**: Failed to convert GraphQL to SQL
- **Execution errors**: SQL query failed to execute
- **Validation errors**: Missing required parameters

## Data Type Conversion

JSON values are automatically converted to SQL types:

- **JSON null** → SQL `NULL`
- **JSON boolean** → SQL `BOOLEAN`
- **JSON number** → SQL `INTEGER` or `NUMERIC`
- **JSON string** → SQL `VARCHAR`

Note: Arrays and objects are not yet supported in parameter values.

## Limitations

The current GraphQL implementation has the following limitations:

1. **Limited type system** - No schema introspection
2. **Simple WHERE clauses** - Only string-based conditions
3. **No subscriptions** - Use REST `/api/subscribe` for real-time updates
4. **No aliases or fragments** - Basic queries only

For more complex operations, use the REST `/api/query` endpoint with raw SQL.

## Examples

### Complete INSERT Example

```bash
curl -X POST http://localhost:8080/api/graphql \
  -H "Content-Type: application/json" \
  -d '{
    "query": "mutation { insert(table: \"posts\", values: {\"title\": \"My Post\", \"author_id\": 1, \"content\": \"Hello World\"}) }"
  }'
```

### Complete SELECT Example

```bash
curl -X POST http://localhost:8080/api/graphql \
  -H "Content-Type: application/json" \
  -d '{
    "query": "{ posts(where: \"author_id = 1\") { id title content } }"
  }'
```

### Combined Query

```bash
curl -X POST http://localhost:8080/api/graphql \
  -H "Content-Type: application/json" \
  -d '{
    "query": "query { posts { id title author_id } users(where: \"status = active\") { id name } health { status } }"
  }'
```

## REST API Comparison

| Feature | GraphQL API | REST API |
|---------|-------------|----------|
| Query Language | GraphQL-like syntax | Raw SQL |
| Data Format | JSON objects with nested relations | JSON arrays |
| Error Handling | GraphQL errors | HTTP status codes |
| Complexity | Simple to moderate queries | Complex SQL |
| Relationships | Auto-resolved via foreign keys | Manual JOINs required |
| Nested Data | Built-in support | Not supported |
| Pagination | Supported on nested queries | Via LIMIT/OFFSET in SQL |
| Real-time Updates | Via REST API | `/api/subscribe` endpoint |

## Migration Guide: REST to GraphQL

### Simple REST Query

```json
POST /api/query
{
  "sql": "SELECT id, name FROM users WHERE active = true"
}
```

Equivalent GraphQL:

```json
POST /api/graphql
{
  "query": "{ users(where: \"active = true\") { id name } }"
}
```

### REST INSERT

```json
POST /api/query
{
  "sql": "INSERT INTO users (name, email) VALUES ($1, $2)",
  "params": ["Alice", "alice@example.com"]
}
```

Equivalent GraphQL:

```json
POST /api/graphql
{
  "query": "mutation { insert(table: \"users\", values: {\"name\": \"Alice\", \"email\": \"alice@example.com\"}) }"
}
```

## Architecture

The GraphQL implementation consists of three main components:

1. **GraphQLRequest/Response Types** - JSON serialization structures
2. **Query Parser** - Converts GraphQL syntax to internal representation
3. **SQL Generator** - Transforms GraphQL queries to SQL statements

The parser is intentionally simple to keep the implementation lightweight. For production use cases requiring full GraphQL features, consider using a dedicated GraphQL library or service.

## Performance Considerations

- **Direct SQL mapping** - GraphQL queries are translated to SQL with minimal overhead
- **Session creation** - Each request creates a new session; connection pooling recommended
- **WHERE clauses** - Complex conditions must be expressed as SQL strings
- **No caching** - Queries are executed immediately with no result caching

## Security

- **SQL Injection Prevention** - Parameter values are type-converted to prevent injection
- **WHERE clause validation** - Currently accepts any SQL expression (validate in application)
- **DELETE safety** - DELETE mutations require a WHERE clause

## Future Enhancements

Potential features for future versions:

- [ ] Full GraphQL schema introspection
- [x] Relationship traversal via foreign keys
- [x] Pagination with limit/offset on nested queries
- [ ] Aliases and query fragments
- [ ] GraphQL subscriptions via WebSocket
- [ ] Query batching
- [ ] Result caching
- [ ] DataLoader pattern for complex relationship queries
