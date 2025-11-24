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

### Prerequisites

**macOS**:
```bash
# Python 3.8+ should already include development headers
python3 --version  # Verify Python is installed
```

**Ubuntu/Debian**:
```bash
sudo apt-get install python3-dev
```

**Fedora/RHEL**:
```bash
sudo dnf install python3-devel
```

### Building the Package

```bash
# 1. Install maturin (Python packaging tool for Rust)
pip install maturin

# 2. Navigate to the Python bindings directory
cd crates/vibesql-python-bindings

# 3. Build and install for local development (recommended for testing)
maturin develop

# 4. Or build a wheel for distribution
maturin build --release
# Wheel will be in: target/wheels/vibesql-0.1.0-*.whl

# 5. Install the wheel
pip install target/wheels/vibesql-0.1.0-*.whl
```

### Quick Build Script

From the repository root:

```bash
./scripts/build-python.sh
```

This script will:
- Check for Python development headers
- Install maturin if needed
- Build the wheel in release mode
- Show installation instructions

### Building with Cargo (Advanced)

If you need to build as part of the workspace:

```bash
# From repository root
cargo build --package vibesql-python-bindings --release
```

Note: This only builds the Rust library, not the Python package. Use maturin for the full Python package.

## Documentation

- [API Documentation](https://docs.rs/vibesql-python-bindings)
- [Main VibeSQL Repository](https://github.com/rjwalters/vibesql)

## License

This project is licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](../../LICENSE-APACHE))
- MIT License ([LICENSE-MIT](../../LICENSE-MIT))

at your option.
