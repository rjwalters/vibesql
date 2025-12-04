# Information Schema Implementation for Drizzle-Kit Support

## Goal
Enable `drizzle-kit pull` to work with VibeSql by implementing PostgreSQL-compatible `information_schema` queries.

## Analysis

Drizzle-kit introspects PostgreSQL using the following key queries:
1. `SELECT * FROM information_schema.tables WHERE table_schema = 'public'` - List all tables
2. `SELECT * FROM information_schema.columns WHERE table_schema = 'public' AND table_name = ?` - Get column info
3. `SELECT * FROM information_schema.table_constraints WHERE table_schema = 'public'` - Get constraints
4. `SELECT * FROM information_schema.key_column_usage WHERE table_schema = 'public'` - Get primary/foreign keys
5. `SELECT * FROM pg_indexes WHERE schemaname = 'public'` - Get indexes

## Implementation Strategy

### Phase 1: Create Information Schema Virtual Tables
Create virtual system tables that expose database metadata:
- `information_schema.schemata` - Schemas/databases
- `information_schema.tables` - Table metadata
- `information_schema.columns` - Column metadata  
- `information_schema.table_constraints` - Constraints
- `information_schema.key_column_usage` - Key column mappings

### Phase 2: Create System Views
Generate these as system views backed by catalog queries (similar to PostgreSQL).

### Phase 3: Support SELECT from information_schema
1. Add schema support for "information_schema" database
2. Implement system view resolution
3. Map queries to catalog metadata retrieval

## Key Files to Modify
- `crates/vibesql-executor/src/executor.rs` - Add information_schema handling
- `crates/vibesql-executor/src/select.rs` - Support system view queries
- `crates/vibesql-catalog/src/lib.rs` - Add metadata query methods
- Parser/AST might need updates for qualified table names

## Test Plan
1. Verify `SELECT * FROM information_schema.tables` returns results
2. Verify `SELECT * FROM information_schema.columns` works with table_schema and table_name filters
3. Test with drizzle-kit pull (if integration available)
4. Document the workflow

## Acceptance Criteria
- ✓ information_schema.tables queries work
- ✓ information_schema.columns queries work
- ✓ Basic WHERE filters work (table_schema, table_name)
- ✓ Results match PostgreSQL format/column names
- ✓ At least one test shows drizzle-kit or similar tools can introspect VibeSql
