# Python Bindings Development Guide

This guide covers building, testing, and distributing the VibeSQL Python package.

## Overview

The Python bindings are in `crates/vibesql-python-bindings/` and provide a DB-API 2.0 compliant interface to VibeSQL.

**Important**: Python bindings are **excluded from the default workspace build** to avoid requiring Python development headers for all developers. They must be built explicitly using the methods below.

## Quick Start

### Building the Package

**Option 1: Use the build script (recommended)**
```bash
./scripts/build-python.sh
```

**Option 2: Use maturin directly**
```bash
cd crates/vibesql-python-bindings
pip install maturin
maturin build --release
```

**Option 3: Development mode (editable install)**
```bash
cd crates/vibesql-python-bindings
pip install maturin
maturin develop
```

## Prerequisites

### macOS
Python 3.8+ should include development headers by default:
```bash
python3 --version  # Should be 3.8 or newer
```

### Linux (Ubuntu/Debian)
```bash
sudo apt-get update
sudo apt-get install python3-dev python3-pip
```

### Linux (Fedora/RHEL)
```bash
sudo dnf install python3-devel python3-pip
```

## Build Artifacts

Maturin produces wheels in:
```
target/wheels/vibesql-0.1.0-cp38-abi3-*.whl
```

The wheel filename includes:
- `cp38-abi3`: Compatible with Python 3.8+ (using stable ABI)
- Platform tag: `macosx_*`, `linux_*`, or `win_*`

## Installation

### From Wheel
```bash
pip install target/wheels/vibesql-*.whl
```

### Development Mode
```bash
cd crates/vibesql-python-bindings
maturin develop
```

Development mode creates an editable install - changes to the Rust code will be reflected after rebuilding with `maturin develop`.

## Testing

### Basic Smoke Test
```python
import vibesql

# Create database connection
db = vibesql.connect()
cursor = db.cursor()

# Run simple query
cursor.execute("SELECT 1 + 1")
result = cursor.fetchone()
assert result == (2,)

print("✓ Python bindings working!")
```

### Run Test Suite
```bash
cd crates/vibesql-python-bindings
pytest tests/  # If you add Python tests
```

## Cargo Workspace Integration

Python bindings are a workspace member but **not** a default member:

```toml
[workspace]
members = [
    # ... all crates including vibesql-python-bindings
]
default-members = [
    # ... all crates EXCEPT vibesql-python-bindings
]
```

This means:
- ✓ `cargo build` - Does NOT build Python bindings (no Python headers required)
- ✓ `cargo build --package vibesql-python-bindings` - Builds just the Rust library
- ✓ `maturin build` - Builds the complete Python package

## Publishing (Future)

When ready to publish to PyPI:

```bash
# Build wheel
cd crates/vibesql-python-bindings
maturin build --release

# Publish to test PyPI first
maturin publish --repository testpypi

# If test looks good, publish to PyPI
maturin publish
```

You'll need PyPI credentials configured in `~/.pypirc` or via environment variables.

## Architecture

### Rust Side (`src/lib.rs`)
- Uses PyO3 to expose Rust functions to Python
- Implements DB-API 2.0 interface (connect, cursor, execute, fetch)
- Handles type conversions between Python and SQL types
- Manages connection pooling and cursors

### Python Side
The package is pure Rust - no Python helper code currently. All functionality is implemented in Rust and exposed via PyO3.

### Type Conversions

| SQL Type | Python Type |
|----------|-------------|
| INTEGER | int |
| DECIMAL | decimal.Decimal |
| VARCHAR/TEXT | str |
| BOOLEAN | bool |
| DATE | datetime.date |
| TIMESTAMP | datetime.datetime |
| NULL | None |

## Troubleshooting

### "Python.h not found"
Install Python development headers:
- macOS: Usually included with Python installation
- Ubuntu: `sudo apt-get install python3-dev`
- Fedora: `sudo dnf install python3-devel`

### "maturin: command not found"
Install maturin:
```bash
pip install maturin
```

### Linking errors during build
Ensure Python development libraries are installed and that PyO3 can find them:
```bash
python3-config --ldflags
```

### Import error: "cannot import name 'vibesql'"
The package name is `vibesql`, not `vibesql-python-bindings`:
```python
import vibesql  # Correct
```

## CI/CD Integration

Add to your CI pipeline to build wheels for distribution:

```yaml
- name: Build Python wheel
  run: |
    pip install maturin
    cd crates/vibesql-python-bindings
    maturin build --release

- name: Upload wheel
  uses: actions/upload-artifact@v3
  with:
    name: python-wheel
    path: target/wheels/*.whl
```

## References

- [PyO3 Documentation](https://pyo3.rs/)
- [Maturin Documentation](https://www.maturin.rs/)
- [Python DB-API 2.0 Specification](https://peps.python.org/pep-0249/)
- [VibeSQL Python Bindings README](../../crates/vibesql-python-bindings/README.md)
