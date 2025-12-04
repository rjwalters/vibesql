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
3. **No pagination** - Use raw SQL queries for large result sets
4. **No relationships** - Foreign key relationships not auto-resolved
5. **No subscriptions** - Use REST `/api/subscribe` for real-time updates
6. **No aliases or fragments** - Basic queries only

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
| Data Format | JSON objects | JSON arrays |
| Error Handling | GraphQL errors | HTTP status codes |
| Complexity | Simple queries | Complex SQL |
| Relationships | Not auto-resolved | Not supported |
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
- [ ] Relationship traversal via foreign keys
- [ ] Pagination with limit/offset
- [ ] Aliases and query fragments
- [ ] GraphQL subscriptions via WebSocket
- [ ] Query batching
- [ ] Result caching
