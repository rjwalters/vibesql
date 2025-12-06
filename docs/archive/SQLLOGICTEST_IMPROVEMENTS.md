# SQLLogicTest Improvement Issues

## Issue 1: Add CREATE INDEX Statement Support

### Title
**feat: Add CREATE INDEX statement support for SQLLogicTest compliance**

### Description
Implement CREATE INDEX statement parsing, AST representation, and basic index management to improve SQLLogicTest passing rate. This addresses 7 failing DDL test files.

### Background
SQLLogicTest suite includes DDL tests that create indexes. Currently, these fail with "expected table, schema, role" parse errors. Adding CREATE INDEX support would:
- Fix 7 failing test files
- Enable index-based query optimization
- Improve overall SQL:1999 compliance

### Implementation Requirements

#### 1. AST Changes (`crates/ast/src/lib.rs`)
- Add `CreateIndex` variant to `Statement` enum
- Define `CreateIndexStatement` struct with fields:
  - `index_name: String`
  - `table_name: String`
  - `columns: Vec<String>` (for simple single-column indexes)
  - `unique: bool` (optional)

#### 2. Parser Changes (`crates/parser/src/parser/create/`)
- Create `index.rs` with `parse_create_index()` function
- Add routing in `create.rs` for `CREATE INDEX` statements
- Support basic syntax: `CREATE [UNIQUE] INDEX index_name ON table_name (column_name)`

#### 3. Executor Changes (`crates/executor/src/`)
- Add `CreateIndexExecutor` with `execute()` method
- Implement basic index creation (in-memory for MVP)
- Return appropriate success/failure status

#### 4. Storage Integration
- Add index metadata storage to `Database` struct
- Basic index lookup structure (HashMap<String, IndexMetadata>)
- No query optimization yet (separate issue)

### Acceptance Criteria
- [ ] `CREATE INDEX idx ON table (col)` parses successfully
- [ ] `CREATE UNIQUE INDEX idx ON table (col)` parses successfully
- [ ] Index creation returns success status
- [ ] SQLLogicTest DDL index tests pass (7 files)
- [ ] No regression in existing functionality

### Testing
- Unit tests for parser
- Integration tests for CREATE INDEX execution
- SQLLogicTest suite validation
- Regression tests for existing CREATE TABLE/etc.

### Priority: High
### Estimate: 4-6 hours
### Risk: Low (isolated feature)

---

## Issue 2: Add CREATE VIEW Statement Support

### Title
**feat: Add CREATE VIEW statement support for SQLLogicTest compliance**

### Description
Implement CREATE VIEW statement parsing, AST representation, and basic view storage to improve SQLLogicTest passing rate. This addresses view-related DDL test failures.

### Background
SQLLogicTest includes tests for view creation and usage. Currently fail with parse errors. Adding CREATE VIEW support enables:
- Query abstraction capabilities
- Compliance with SQL:1999 view features
- Better test coverage

### Implementation Requirements

#### 1. AST Changes
- Add `CreateView` variant to `Statement` enum
- Define `CreateViewStatement` struct:
  - `view_name: String`
  - `column_names: Option<Vec<String>>` (optional column list)
  - `query: Box<SelectStatement>` (the view definition query)

#### 2. Parser Changes
- Create `view.rs` with `parse_create_view()` function
- Support syntax: `CREATE VIEW view_name [(col1, col2, ...)] AS SELECT ...`

#### 3. Executor Changes
- Add `CreateViewExecutor`
- Store view definition in database metadata
- Basic view validation (table references exist)

#### 4. Storage Integration
- Add view metadata to `Database` struct
- View definition storage and retrieval

### Acceptance Criteria
- [ ] `CREATE VIEW v AS SELECT * FROM t` parses and executes
- [ ] `CREATE VIEW v (a,b) AS SELECT x,y FROM t` with column list
- [ ] View creation returns success status
- [ ] SQLLogicTest view-related tests pass
- [ ] No regression in existing functionality

### Testing
- Parser unit tests
- View creation integration tests
- SQLLogicTest validation
- Regression testing

### Priority: High
### Estimate: 4-6 hours
### Risk: Medium (query parsing integration)

---

## Issue 3: Graceful Spatial Data Type Handling

### Title
**feat: Add graceful handling for spatial data types in SQLLogicTest**

### Description
Modify data type parser to handle spatial/geometric types (MULTIPOLYGON, POINT, etc.) gracefully instead of failing with parse errors. This prevents test suite failures on unsupported but valid SQL extensions.

### Background
SQLLogicTest includes tests using spatial data types from SQL/MM standard. These are outside SQL:1999 scope but shouldn't cause parse failures. Current behavior fails tests with "Unknown data type: MULTIPOLYGON".

### Implementation Requirements

#### 1. Parser Changes (`crates/parser/src/parser/create/types.rs`)
- Modify `parse_data_type()` function
- For unknown data types, check if they match spatial type patterns
- Return `DataType::UserDefined { type_name }` instead of error

#### 2. Spatial Type Detection
- Common spatial types to handle gracefully:
  - MULTIPOLYGON, POLYGON, MULTILINESTRING, LINESTRING
  - MULTIPOINT, POINT, GEOMETRY, GEOMETRYCOLLECTION
- Use pattern matching or allowlist

#### 3. Type System Integration
- Ensure `UserDefined` types work in CREATE TABLE statements
- Add basic validation for user-defined type names

### Acceptance Criteria
- [ ] `CREATE TABLE t (col MULTIPOLYGON)` doesn't fail parsing
- [ ] Returns `DataType::UserDefined` for spatial types
- [ ] SQLLogicTest spatial type tests skip gracefully instead of failing
- [ ] No impact on valid SQL:1999 data types

### Testing
- Parser tests for spatial type handling
- SQLLogicTest validation (should skip, not fail)
- Regression tests for standard data types

### Priority: High
### Estimate: 1-2 hours
### Risk: Low (backward compatible change)

---

## Issue 4: Add Backtick Identifier Support

### Title
**feat: Add backtick identifier support for MySQL-style quoting**

### Description
Implement backtick-quoted identifiers (`table_name`, `column_name`) to support MySQL-style syntax in SQLLogicTest suite.

### Background
Some SQLLogicTest files use MySQL-style backtick-quoted identifiers. Currently fails with lexer errors. This is a small syntax addition with big compatibility impact.

### Implementation Requirements

#### 1. Lexer Changes (`crates/lexer/src/lib.rs`)
- Modify token recognition to handle backtick-delimited identifiers
- Add `BacktickIdentifier(String)` token type
- Handle escaping within backticks if needed

#### 2. Parser Integration
- Update identifier parsing to accept backtick tokens
- Convert to standard identifier representation
- Maintain compatibility with unquoted identifiers

### Acceptance Criteria
- [ ] `` `table_name` `` parses as identifier
- [ ] `` `column name with spaces` `` supported
- [ ] Backtick identifiers work in CREATE TABLE, SELECT, etc.
- [ ] SQLLogicTest backtick tests pass

### Testing
- Lexer tests for backtick handling
- Parser integration tests
- SQLLogicTest validation

### Priority: Medium
### Estimate: 2-3 hours
### Risk: Low (additive syntax change)
