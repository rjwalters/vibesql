# VibeSQL Crates

This directory contains all the individual crates that make up the VibeSQL database engine.

## Crate Organization

### Core Engine Crates

- **`vibesql-types/`** - SQL:1999 type system
  - Complete data type implementation (INTEGER, VARCHAR, BOOLEAN, DATE, TIMESTAMP, NUMERIC, etc.)
  - Type checking and coercion rules
  - Spatial/geometric types (POINT, LINESTRING, POLYGON, etc.)
  - Full numeric precision handling

- **`vibesql-ast/`** - Abstract Syntax Tree definitions
  - SQL statement AST nodes (DDL, DML, DCL)
  - Expression AST nodes
  - Type-safe tree representation
  - Visitor pattern support

- **`vibesql-parser/`** - SQL:1999 parser
  - Hand-written lexer/tokenizer with full SQL:1999 syntax
  - Recursive descent parser
  - Comprehensive error reporting with position tracking
  - Support for advanced features (CTEs, window functions, spatial queries)

- **`vibesql-catalog/`** - Schema and metadata management
  - Database catalog with full schema support
  - Table, view, and index metadata
  - Information schema views (INFORMATION_SCHEMA)
  - Foreign key relationship tracking
  - Stored procedure/function registry

- **`vibesql-storage/`** - Storage engine
  - In-memory row storage with B-tree indexes
  - Spatial indexes (R-tree) for geometric data
  - Full-text indexes for text search
  - Efficient scan and lookup operations
  - Iterator-based execution support

- **`vibesql-executor/`** - Query execution engine
  - Complete statement executors (SELECT, INSERT, UPDATE, DELETE, MERGE, TRUNCATE)
  - Advanced expression evaluator with 200+ built-in functions
  - Multiple join algorithms (hash join, nested loop, merge join)
  - Aggregate and window functions
  - Subquery execution (correlated and uncorrelated)
  - Common Table Expressions (CTEs)
  - Query optimization (predicate pushdown, projection pruning)
  - Transaction support with MVCC

### Interface Crates

- **`vibesql-cli/`** - Command-line interface
  - Interactive SQL shell (REPL)
  - PostgreSQL-compatible meta-commands (\d, \dt, \l, etc.)
  - Multiple output formats (table, CSV, JSON, XML, HTML)
  - Import/export functionality
  - Query history and editing

- **`vibesql-server/`** - Network server
  - PostgreSQL wire protocol implementation
  - Remote client connection support
  - Async I/O with Tokio
  - Authentication and session management
  - Compatible with psql, JDBC, ODBC, and other PostgreSQL clients

- **`vibesql-wasm-bindings/`** - WebAssembly bindings
  - WASM-compatible API for browser execution
  - JavaScript interop layer
  - Powers the live web demo at https://vibesql.org/

- **`vibesql-python-bindings/`** - Python bindings
  - Python API for embedding VibeSQL
  - DB-API 2.0 compatible interface
  - PyO3-based implementation

### Replication

- **`vibesql-consensus/`** - Raft-based replication
  - Single-group whole-database replication built on `openraft`
  - Replicated state machine applies committed transactions from the Raft log
  - See [docs/decisions/0004-consensus-library.md](../docs/decisions/0004-consensus-library.md)

### Supporting Crates

- **`vibesql-l10n/`** - Localization and message catalogs
- **`vibesql-bench-common/`** - Shared benchmark harness utilities

### Testing Infrastructure

- **`vibesql-sqllogictest/`** - SQL conformance testing
  - SQLLogicTest parser and runner
  - 622/622 test files passing (100%, ~7.4M tests)
  - Validates SQL:1999 compliance

## Building

```bash
# Build all crates
cargo build

# Build specific crate
cargo build -p vibesql-executor

# Test all crates
cargo test

# Test specific crate
cargo test -p vibesql-types

# Run SQLLogicTest suite
cargo test --test sqllogictest_suite

# Check without building (fast!)
cargo check

# Build CLI
cargo build -p vibesql-cli --release

# Build WASM bindings
wasm-pack build crates/vibesql-wasm-bindings
```

## Crate Dependencies

The crates form a layered architecture with clear dependency flow:

```
┌─────────────────────────────────────────────────┐
│  Interface Layer                                │
│  vibesql-cli, vibesql-wasm-bindings,           │
│  vibesql-python-bindings                        │
└────────────────┬────────────────────────────────┘
                 │
┌────────────────▼────────────────────────────────┐
│  Execution Layer                                │
│  vibesql-executor ◄─── sqllogictest            │
└────────┬────────────────────────────────────────┘
         │
┌────────▼────────┬───────────────────────────────┐
│                 │                               │
│  vibesql-       │  vibesql-       vibesql-     │
│  storage        │  catalog        parser       │
│                 │       │            │          │
└─────────────────┴───────┴────────────┴──────────┘
                          │            │
                 ┌────────▼────────────▼─────────┐
                 │  Foundation Layer             │
                 │  vibesql-types, vibesql-ast   │
                 └───────────────────────────────┘
```

**Key relationships:**
- `vibesql-types` and `vibesql-ast` are foundational with no internal dependencies
- `vibesql-parser` depends on types and ast
- `vibesql-catalog` depends on types and ast
- `vibesql-storage` depends on types and catalog
- `vibesql-executor` depends on all core crates and orchestrates everything
- Interface crates depend on executor for complete functionality

## Development Status

✅ **Complete and Production-Ready**
- All core SQL:1999 features implemented
- 100% sqltest conformance (739/739 mandatory tests)
- 100% SQLLogicTest coverage (622/622 files, ~7.4M tests)
- 7,000+ unit tests across all crates
- Comprehensive documentation

## Publishing to crates.io

VibeSQL crates are published to [crates.io](https://crates.io) for use by the Rust community.

### Publishing Order

Due to path dependencies, crates must be published sequentially in this order:

1. **Foundation Layer** (no dependencies):
   - `vibesql-types`
   - `vibesql-ast`

2. **Middle Layer** (depends on foundation):
   - `vibesql-parser`
   - `vibesql-catalog`

3. **Storage Layer**:
   - `vibesql-storage`

4. **Execution Layer**:
   - `vibesql-executor`

5. **Interface Layer**:
   - `vibesql-consensus`
   - `vibesql-l10n`
   - `vibesql-bench-common`
   - `vibesql-cli`
   - `vibesql-wasm-bindings`
   - `vibesql-python-bindings`

6. **Root Package**:
   - `vibesql`

**Note**: The `sqllogictest` crate is marked `publish = false` as it contains vibesql-specific patches and is used only for internal testing.

### Publishing Commands

```bash
# Verify a crate is ready for publishing (dry run)
cargo publish --dry-run -p vibesql-types

# Publish a crate to crates.io
cargo publish -p vibesql-types

# After publishing, wait for crates.io to index before publishing dependents
# (usually takes 1-2 minutes)
```

### Pre-Publishing Checklist

Before publishing, ensure:

- ✅ All tests pass: `cargo test --all`
- ✅ Documentation builds without warnings: `cargo doc --no-deps --all`
- ✅ README files are present for all crates
- ✅ Version numbers are updated in Cargo.toml
- ✅ CHANGELOG is updated with release notes
- ✅ All rustdoc warnings are fixed
- ✅ Dependencies use version ranges (not path dependencies after publishing)

### Available on crates.io

Once published, crates can be used by adding to your `Cargo.toml`:

```toml
[dependencies]
vibesql-types = "0.2"
vibesql-parser = "0.2"
vibesql-executor = "0.2"
# etc.
```

## Documentation

- Each crate contains inline documentation accessible via `cargo doc`
- Build documentation: `cargo doc --no-deps --open`
- Published documentation available at [docs.rs/vibesql](https://docs.rs/vibesql)
- See main [README.md](../README.md) for project overview
