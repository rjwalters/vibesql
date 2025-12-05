# Feature Status

This document provides a detailed breakdown of implemented features in VibeSQL as of December 2025.

## Query Engine

- **Full SQL Support**: SELECT, INSERT, UPDATE, DELETE with all standard clauses
- **All JOIN types**: INNER, LEFT, RIGHT, FULL OUTER, CROSS
- **Advanced features**: Subqueries (scalar, table, correlated), CTEs (recursive), window functions
- **Set operations**: UNION, INTERSECT, EXCEPT (with ALL variants)
- **Aggregates**: COUNT, SUM, AVG, MIN, MAX with GROUP BY/HAVING
- **Window functions**: ROW_NUMBER, RANK, DENSE_RANK, NTILE, LAG, LEAD, FIRST_VALUE, LAST_VALUE, aggregate OVER()

## DDL & Transactions

- **Schema operations**: CREATE/DROP TABLE, CREATE/DROP SCHEMA, CREATE/DROP VIEW, ALTER TABLE
- **Index types**: B-tree, R-tree (spatial), FULLTEXT, HNSW (vector), IVFFlat (vector)
- **Transactions**: BEGIN, COMMIT, ROLLBACK, SAVEPOINT (nested transactions)
- **Constraints**: NOT NULL, PRIMARY KEY, UNIQUE, CHECK, FOREIGN KEY (all fully enforced)

## Security & Privileges

- **Role management**: CREATE/DROP ROLE
- **Access control**: GRANT/REVOKE with full privilege enforcement
- **Supported privileges**: SELECT, INSERT, UPDATE, DELETE on tables and schemas

## Server & APIs

- **PostgreSQL wire protocol**: Compatible with psql, JDBC, ODBC, and other PostgreSQL clients
- **HTTP REST API**: Full CRUD operations with JSON responses
- **GraphQL API**: Schema introspection and queries
- **Real-time subscriptions**: Convex-like reactivity with delta updates
- **Server-Sent Events (SSE)**: HTTP streaming for subscription updates
- **WebSocket support**: Persistent connections for real-time data

## Built-in Functions (200+)

- **String**: UPPER, LOWER, SUBSTRING, TRIM, CHAR_LENGTH, POSITION, CONCAT, etc.
- **Date/Time**: CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP, EXTRACT, date arithmetic
- **Math**: ABS, CEILING, FLOOR, SQRT, POWER, trigonometric, logarithmic functions
- **Conditional**: CASE, COALESCE, NULLIF, GREATEST, LEAST
- **Type conversion**: CAST
- **Spatial**: ST_Distance, ST_Contains, ST_Intersects, ST_Area, ST_Buffer, etc.
- **Full-text**: MATCH AGAINST for text search
- **Vector**: vector_distance (cosine, euclidean, dot product)

## Type System

- **Numeric**: INTEGER, SMALLINT, BIGINT, FLOAT, REAL, DOUBLE PRECISION
- **String**: VARCHAR(n), CHAR(n), TEXT
- **Temporal**: DATE, TIME, TIMESTAMP
- **Other**: BOOLEAN, NUMERIC(p,s), DECIMAL(p,s)
- **Spatial**: POINT, LINESTRING, POLYGON, GEOMETRY
- **Vector**: VECTOR(n) for AI/ML embeddings
- **Three-valued logic**: Proper NULL propagation

## Extended Features

- **Vector search**: VECTOR(n) types with similarity search (cosine, euclidean, dot product)
- **Vector indexes**: HNSW and IVFFlat for approximate nearest neighbor
- **File storage**: Blob storage with STORAGE_URL, STORAGE_SIZE functions
- **Scheduled functions**: SCHEDULE AFTER/AT, CREATE CRON for deferred execution
- **Stored procedures**: CREATE PROCEDURE/FUNCTION with IN/OUT/INOUT parameters
- **Triggers**: BEFORE/AFTER triggers on INSERT/UPDATE/DELETE

## Operators & Predicates

- **Comparison**: =, <>, <, >, <=, >=
- **Logical**: AND, OR, NOT
- **Special**: BETWEEN, IN, LIKE, EXISTS, IS NULL/IS NOT NULL
- **Quantified**: ALL, ANY, SOME (with subqueries)
- **Arithmetic**: +, -, *, /, %
- **String**: || (concatenation)
- **Vector**: <-> (distance operators)

## Query Optimization

- **Columnar execution**: SIMD-accelerated analytical queries
- **Join optimization**: Cost-based join reordering, hash joins, nested loop joins
- **Predicate pushdown**: Filters pushed to table scans
- **Subquery transformation**: Correlated subqueries converted to joins
- **Parallel execution**: Hardware-aware parallelism for scans, joins, aggregation, sorting
- **Query caching**: Plan caching and result caching with automatic invalidation

## CLI & Tools

- **Interactive REPL**: Full-featured SQL shell with readline and history
- **Execution modes**: Interactive, command (-c), file (-f), stdin
- **Meta-commands**: PostgreSQL-compatible \d, \dt, \ds, \di, \du
- **Import/Export**: \copy command for CSV and JSON
- **Output formats**: Table, JSON, CSV, Markdown, HTML
- **Configuration**: ~/.vibesqlrc with TOML format
- **Persistence**: \save command for SQL dumps

## Bindings & SDKs

- **WASM**: Runs in browser with live demo
- **Python**: DB-API 2.0 compatible interface via PyO3
- **TypeScript SDK**: React hooks (useSubscription, useQuery)
- **Drizzle ORM**: Type-safe query adapter

## Test Coverage

- **Unit tests**: 4,800+ tests
- **SQLLogicTest**: 623 files (~5.9M individual tests), 100% pass rate
- **SQL:1999 sqltest**: 739/739 (100%)
- **TPC-H**: 22/22 queries
- **TPC-C**: All OLTP transactions
- **TPC-DS**: 97/99 queries

## See Also

- [SQL:1999 Conformance Report](https://rjwalters.github.io/vibesql/conformance.html) - Detailed conformance test results
- [Roadmap](../ROADMAP.md) - Future development plans
- [CLI Guide](../CLI_GUIDE.md) - Complete CLI documentation
- [HTTP API](../http-api.md) - REST and GraphQL endpoints
- [Vector Search](../vector-search.md) - AI/ML embedding support
