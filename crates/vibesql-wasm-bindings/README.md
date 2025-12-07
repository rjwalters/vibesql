# vibesql-wasm-bindings

WebAssembly bindings for VibeSQL SQL database engine.

## Overview

This crate provides WebAssembly bindings that allow VibeSQL to run in web browsers and other JavaScript environments. It exposes a clean JavaScript API for executing SQL queries and managing databases.

## Features

- **Browser Support**: Runs entirely in the browser with no server required
- **OPFS Storage**: Persistent storage using Origin Private File System
- **Full SQL Support**: Complete SQL:1999 implementation in WASM
- **Type-Safe API**: Strongly-typed JavaScript interface
- **JSON Results**: Query results serialized as JSON for easy integration
- **Schema Management**: Create tables, indexes, views, and more
- **Transaction Support**: Full ACID transaction support in the browser

## Installation

```bash
npm install vibesql-wasm
```

## Usage

```javascript
import init, { VibeSQL } from 'vibesql-wasm';

// Initialize WASM module
await init();

// Create database instance
const db = new VibeSQL();

// Execute DDL
await db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(50))");

// Insert data
await db.execute("INSERT INTO users VALUES (1, 'Alice')");

// Query data
const result = await db.query("SELECT * FROM users");
console.log(result.rows);
```

## Building

Build from the repository root using the provided script:

```bash
# From repository root
./scripts/build-wasm.sh

# Output location: web-demo/public/pkg/
```

The build script handles:
- macOS LLVM configuration (required for wasm32 target)
- Size-optimized release settings
- Correct output directory for the web demo

See [web-demo/README.md](../../web-demo/README.md) for full development setup.

## Documentation

- [API Documentation](https://docs.rs/vibesql-wasm-bindings)
- [Main VibeSQL Repository](https://github.com/rjwalters/vibesql)
- [WASM Examples](https://github.com/rjwalters/vibesql/tree/main/examples/wasm)

## License

This project is licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](../../LICENSE-APACHE))
- MIT License ([LICENSE-MIT](../../LICENSE-MIT))

at your option.
