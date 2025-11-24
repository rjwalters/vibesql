# vibesql-python-bindings

Python bindings for VibeSQL SQL database engine.

## Overview

This crate provides Python bindings following DB-API 2.0 conventions, allowing VibeSQL to be used from Python applications for testing, benchmarking, and integration.

## Features

- **DB-API 2.0 Compliant**: Standard Python database interface
- **Type Conversions**: Automatic conversion between Python and SQL types
- **Connection Pooling**: Efficient connection management
- **Cursor Support**: Full cursor API with fetchone/fetchmany/fetchall
- **Performance Profiling**: Built-in profiling utilities for benchmarking
- **Native Speed**: Zero-copy access to Rust implementation

## Installation

```bash
pip install vibesql
```

## Usage

```python
import vibesql

# Create a connection
db = vibesql.connect()

# Get a cursor
cursor = db.cursor()

# Execute DDL
cursor.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(50))")

# Insert data
cursor.execute("INSERT INTO users VALUES (1, 'Alice')")
cursor.execute("INSERT INTO users VALUES (2, 'Bob')")

# Query data
cursor.execute("SELECT * FROM users WHERE id > ?", [1])
result = cursor.fetchall()
print(result)  # [(2, 'Bob')]

# Close connection
db.close()
```

## Context Manager Support

```python
import vibesql

with vibesql.connect() as db:
    cursor = db.cursor()
    cursor.execute("SELECT 1")
    print(cursor.fetchone())
```

## Building

**Note**: Python bindings are excluded from the default workspace build to avoid requiring Python development headers. To build the Python bindings, you need Python 3.8+ and development headers installed.

```bash
# Install maturin
pip install maturin

# Build and install locally (from this directory)
cd crates/vibesql-python-bindings
maturin develop

# Or build wheel
maturin build --release

# To build as part of workspace (requires Python dev environment)
cargo build --package vibesql-python-bindings
```

## Documentation

- [API Documentation](https://docs.rs/vibesql-python-bindings)
- [Main VibeSQL Repository](https://github.com/rjwalters/vibesql)

## License

This project is licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](../../LICENSE-APACHE))
- MIT License ([LICENSE-MIT](../../LICENSE-MIT))

at your option.
