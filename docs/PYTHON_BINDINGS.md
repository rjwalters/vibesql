# VibeSQL Python Bindings Guide

Complete guide to using VibeSQL from Python with the DB-API 2.0 compatible interface.

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [API Reference](#api-reference)
  - [Module Functions](#module-functions)
  - [Database Class](#database-class)
  - [Cursor Class](#cursor-class)
  - [Exception Types](#exception-types)
- [Working with Data](#working-with-data)
- [Database Persistence](#database-persistence)
- [Performance Optimization](#performance-optimization)
- [Transaction Support](#transaction-support)
- [Examples](#examples)

---

## Installation

### Prerequisites

- Python 3.8 or higher
- Rust toolchain (for building from source)
- maturin (Python package)

### Build and Install

```bash
# Install maturin
pip install maturin

# Build and install Python bindings (development mode)
cd /path/to/vibesql
maturin develop

# Or build a release wheel
maturin build --release
pip install target/wheels/vibesql-*.whl
```

### Verify Installation

```python
import vibesql
print(vibesql.Database().version())
```

---

## Quick Start

```python
import vibesql

# Create a database connection
db = vibesql.connect()

# Get a cursor
cursor = db.cursor()

# Create a table
cursor.execute("CREATE TABLE users (id INTEGER, name VARCHAR(100), age INTEGER)")

# Insert data
cursor.execute("INSERT INTO users VALUES (1, 'Alice', 30)")
cursor.execute("INSERT INTO users VALUES (2, 'Bob', 25)")
cursor.execute("INSERT INTO users VALUES (3, 'Charlie', 35)")

# Query data
cursor.execute("SELECT name, age FROM users WHERE age > 25")
results = cursor.fetchall()

for row in results:
    print(f"Name: {row[0]}, Age: {row[1]}")
# Output:
# Name: Alice, Age: 30
# Name: Charlie, Age: 35

# Clean up
cursor.close()
db.close()
```

---

## API Reference

### Module Functions

#### `vibesql.connect()`

Creates a new database connection.

**Returns:** `Database` instance

**Example:**
```python
db = vibesql.connect()
```

#### `vibesql.enable_profiling()`

Enables detailed performance profiling output to stderr.

**Example:**
```python
vibesql.enable_profiling()
# Now all queries will print timing information
```

#### `vibesql.disable_profiling()`

Disables performance profiling output.

**Example:**
```python
vibesql.disable_profiling()
```

---

### Database Class

The `Database` class represents a connection to an in-memory VibeSQL database.

#### Constructor

```python
db = vibesql.Database()  # Alternative to vibesql.connect()
```

#### Methods

##### `cursor() -> Cursor`

Creates a new cursor for executing queries.

**Returns:** `Cursor` instance

**Example:**
```python
cursor = db.cursor()
```

##### `close() -> None`

Closes the database connection. For in-memory databases, this is a no-op but provided for DB-API 2.0 compatibility.

**Example:**
```python
db.close()
```

##### `version() -> str`

Returns the version string of the Python bindings.

**Returns:** Version string

**Example:**
```python
print(db.version())  # "vibesql-py 0.1.0"
```

##### `save(path: str) -> None`

Saves the database to a SQL dump file.

**Arguments:**
- `path`: Path where the SQL dump file will be created

**Raises:**
- `IOError`: If file cannot be written

**Example:**
```python
db = vibesql.connect()
cursor = db.cursor()
cursor.execute("CREATE TABLE products (id INTEGER, name VARCHAR(100))")
cursor.execute("INSERT INTO products VALUES (1, 'Widget')")
db.save("/tmp/mydb.sql")
```

##### `Database.load(path: str) -> Database` (static method)

Loads a database from a SQL dump file.

**Arguments:**
- `path`: Path to the SQL dump file to load

**Returns:** New `Database` instance with loaded state

**Raises:**
- `IOError`: If file cannot be read or SQL execution fails

**Example:**
```python
# Load a previously saved database
db = vibesql.Database.load("/tmp/mydb.sql")
cursor = db.cursor()
cursor.execute("SELECT * FROM products")
print(cursor.fetchall())
```

---

### Cursor Class

The `Cursor` class is used to execute SQL statements and fetch results.

#### Methods

##### `execute(sql: str, params: tuple = None) -> None`

Executes a SQL statement with optional parameter binding.

**Arguments:**
- `sql`: SQL statement to execute (may contain `?` placeholders)
- `params`: Optional tuple of parameter values to bind to `?` placeholders

**Raises:**
- `ProgrammingError`: For SQL parse errors
- `OperationalError`: For execution errors

**Examples:**
```python
# Simple execution
cursor.execute("CREATE TABLE users (id INTEGER, name VARCHAR(50))")

# With parameter binding
cursor.execute("INSERT INTO users VALUES (?, ?)", (1, 'Alice'))
cursor.execute("SELECT * FROM users WHERE id = ?", (1,))
```

**Supported Statement Types:**
- `SELECT` - Query data
- `INSERT` - Insert rows
- `UPDATE` - Update rows
- `DELETE` - Delete rows
- `CREATE TABLE` - Create tables
- `DROP TABLE` - Drop tables
- `CREATE VIEW` - Create views
- `DROP VIEW` - Drop views
- Other DDL/DML statements (see documentation)

##### `fetchall() -> list[tuple]`

Fetches all remaining rows from the last query result.

**Returns:** List of tuples, each representing a row

**Raises:**
- `ProgrammingError`: If last statement was not a SELECT query

**Example:**
```python
cursor.execute("SELECT * FROM users")
rows = cursor.fetchall()
for row in rows:
    print(row)  # (1, 'Alice'), (2, 'Bob'), etc.
```

##### `fetchone() -> tuple | None`

Fetches the next row from the last query result.

**Returns:** Tuple representing the next row, or `None` if no more rows

**Raises:**
- `ProgrammingError`: If last statement was not a SELECT query

**Example:**
```python
cursor.execute("SELECT * FROM users")
while True:
    row = cursor.fetchone()
    if row is None:
        break
    print(row)
```

##### `fetchmany(size: int) -> list[tuple]`

Fetches multiple rows from the last query result.

**Arguments:**
- `size`: Number of rows to fetch

**Returns:** List of tuples (may contain fewer rows if less are available)

**Raises:**
- `ProgrammingError`: If last statement was not a SELECT query

**Example:**
```python
cursor.execute("SELECT * FROM large_table")
while True:
    batch = cursor.fetchmany(100)
    if not batch:
        break
    process_batch(batch)
```

##### `close() -> None`

Closes the cursor. For now, this is a no-op but provided for DB-API 2.0 compatibility.

**Example:**
```python
cursor.close()
```

##### `cache_stats() -> (int, int, float)`

Returns statistics about the statement cache.

**Returns:** Tuple of `(hits, misses, hit_rate)`

**Example:**
```python
hits, misses, rate = cursor.cache_stats()
print(f"Cache hit rate: {rate:.2%}")
```

##### `schema_cache_stats() -> (int, int, float)`

Returns statistics about the schema cache.

**Returns:** Tuple of `(hits, misses, hit_rate)`

**Example:**
```python
hits, misses, rate = cursor.schema_cache_stats()
print(f"Schema cache hit rate: {rate:.2%}")
```

##### `clear_cache() -> None`

Clears both statement and schema caches.

**Example:**
```python
cursor.clear_cache()
```

#### Properties

##### `rowcount -> int`

Number of rows affected by the last operation.

**Returns:**
- For DML operations (INSERT, UPDATE, DELETE): number of rows affected
- For SELECT queries: number of rows in result set
- `-1` if no query has been executed

**Example:**
```python
cursor.execute("UPDATE users SET age = age + 1")
print(f"Updated {cursor.rowcount} rows")
```

---

### Exception Types

All exceptions inherit from Python's standard `Exception` class and follow DB-API 2.0 conventions.

#### `vibesql.DatabaseError`

Base class for all database-related errors.

#### `vibesql.OperationalError`

Raised for errors related to database operation (e.g., table not found, constraint violation).

**Example:**
```python
try:
    cursor.execute("INSERT INTO users VALUES (1, 'Alice')")
    cursor.execute("INSERT INTO users VALUES (1, 'Bob')")  # Duplicate key
except vibesql.OperationalError as e:
    print(f"Database error: {e}")
```

#### `vibesql.ProgrammingError`

Raised for programming errors (e.g., SQL syntax errors, wrong number of parameters).

**Example:**
```python
try:
    cursor.execute("SELCT * FROM users")  # Typo: SELCT instead of SELECT
except vibesql.ProgrammingError as e:
    print(f"SQL error: {e}")
```

---

## Working with Data

### Data Type Mapping

VibeSQL automatically converts between Python and SQL types:

| SQL Type | Python Type |
|----------|-------------|
| INTEGER, SMALLINT, BIGINT | `int` |
| FLOAT, REAL, DOUBLE PRECISION | `float` |
| VARCHAR, CHAR, TEXT | `str` |
| BOOLEAN | `bool` |
| NULL | `None` |
| DATE, TIME, TIMESTAMP | `str` (string representation) |

**Example:**
```python
cursor.execute("""
    CREATE TABLE mixed_types (
        i INTEGER,
        f FLOAT,
        s VARCHAR(100),
        b BOOLEAN,
        n INTEGER
    )
""")

cursor.execute("INSERT INTO mixed_types VALUES (?, ?, ?, ?, ?)",
               (42, 3.14, 'hello', True, None))

cursor.execute("SELECT * FROM mixed_types")
row = cursor.fetchone()
print(row)  # (42, 3.14, 'hello', True, None)
```

### NULL Handling

SQL `NULL` values are mapped to Python `None`:

```python
cursor.execute("SELECT NULL AS value")
row = cursor.fetchone()
print(row[0] is None)  # True
```

### Parameter Binding

Use `?` placeholders for safe parameter binding:

```python
# CORRECT: Using parameter binding
name = "Alice'; DROP TABLE users; --"
cursor.execute("SELECT * FROM users WHERE name = ?", (name,))

# WRONG: String interpolation (SQL injection risk!)
cursor.execute(f"SELECT * FROM users WHERE name = '{name}'")
```

---

## Database Persistence

VibeSQL supports saving and loading database state using SQL dump files.

### Saving a Database

```python
db = vibesql.connect()
cursor = db.cursor()

# Create schema and data
cursor.execute("CREATE TABLE employees (id INTEGER, name VARCHAR(100), salary FLOAT)")
cursor.execute("INSERT INTO employees VALUES (1, 'Alice', 75000)")
cursor.execute("INSERT INTO employees VALUES (2, 'Bob', 65000)")

# Save to file
db.save("company.sql")
```

### Loading a Database

```python
# Load from file (static method)
db = vibesql.Database.load("company.sql")

# Query the loaded data
cursor = db.cursor()
cursor.execute("SELECT * FROM employees")
print(cursor.fetchall())
# [(1, 'Alice', 75000.0), (2, 'Bob', 65000.0)]
```

### Use Cases

- **Backup and restore**: Save database state periodically
- **Testing**: Save known-good state and reload between tests
- **Data exchange**: Share database snapshots as SQL files
- **Version control**: Track schema changes in Git

---

## Performance Optimization

### Statement Caching

The cursor automatically caches up to 1,000 parsed SQL statements using an LRU cache:

```python
cursor = db.cursor()

# First execution: cache miss, statement is parsed
cursor.execute("SELECT * FROM users WHERE id = ?", (1,))

# Second execution: cache hit, reuses parsed AST
cursor.execute("SELECT * FROM users WHERE id = ?", (2,))

# Check cache statistics
hits, misses, rate = cursor.cache_stats()
print(f"Statement cache hit rate: {rate:.2%}")
```

### Schema Caching

Table schemas are cached to reduce catalog lookups:

```python
# First UPDATE: fetches schema from catalog
cursor.execute("UPDATE users SET age = 30 WHERE id = 1")

# Second UPDATE: reuses cached schema
cursor.execute("UPDATE users SET age = 31 WHERE id = 2")

# Check schema cache statistics
hits, misses, rate = cursor.schema_cache_stats()
print(f"Schema cache hit rate: {rate:.2%}")
```

### Profiling

Enable detailed profiling to identify bottlenecks:

```python
import vibesql

vibesql.enable_profiling()

db = vibesql.connect()
cursor = db.cursor()
cursor.execute("SELECT * FROM large_table")
# Profiling output printed to stderr:
#   [execute()] SQL string copied: 0.123ms
#   [execute()] Acquired cache lock: 0.045ms
#   [execute()] Cache MISS - need to parse: 0.012ms
#   ... etc

vibesql.disable_profiling()
```

### Best Practices

1. **Reuse cursors** for multiple queries to benefit from caching
2. **Use parameter binding** instead of string formatting
3. **Batch operations** when possible (e.g., multi-row INSERT)
4. **Clear caches** after schema changes if using same cursor
5. **Monitor cache hit rates** to ensure effective caching

---

## Transaction Support

VibeSQL supports full ACID transactions (except durability, as it's an in-memory database).

### Basic Transactions

```python
cursor.execute("BEGIN TRANSACTION")

try:
    cursor.execute("INSERT INTO accounts VALUES (1, 'Alice', 1000)")
    cursor.execute("INSERT INTO accounts VALUES (2, 'Bob', 500)")
    cursor.execute("COMMIT")
except Exception as e:
    cursor.execute("ROLLBACK")
    print(f"Transaction failed: {e}")
```

### Savepoints

```python
cursor.execute("BEGIN TRANSACTION")

# First operation
cursor.execute("INSERT INTO users VALUES (1, 'Alice')")
cursor.execute("SAVEPOINT sp1")

# Second operation (might fail)
try:
    cursor.execute("INSERT INTO users VALUES (1, 'Bob')")  # Duplicate key!
except vibesql.OperationalError:
    cursor.execute("ROLLBACK TO SAVEPOINT sp1")  # Undo second operation
    cursor.execute("INSERT INTO users VALUES (2, 'Bob')")  # Fixed

cursor.execute("COMMIT")
```

### Context Manager Pattern

```python
class Transaction:
    def __init__(self, cursor):
        self.cursor = cursor

    def __enter__(self):
        self.cursor.execute("BEGIN TRANSACTION")
        return self.cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.cursor.execute("COMMIT")
        else:
            self.cursor.execute("ROLLBACK")
        return False

# Usage
with Transaction(cursor):
    cursor.execute("INSERT INTO users VALUES (1, 'Alice')")
    cursor.execute("UPDATE accounts SET balance = balance - 100 WHERE id = 1")
```

---

## Examples

### Example 1: Data Analysis

```python
import vibesql

# Load dataset
db = vibesql.Database.load("sales_data.sql")
cursor = db.cursor()

# Aggregate query
cursor.execute("""
    SELECT
        region,
        COUNT(*) as order_count,
        SUM(amount) as total_sales,
        AVG(amount) as avg_order
    FROM sales
    WHERE year = 2024
    GROUP BY region
    ORDER BY total_sales DESC
""")

print("Sales by Region (2024):")
for row in cursor.fetchall():
    region, count, total, avg = row
    print(f"  {region}: {count} orders, ${total:,.2f} total, ${avg:.2f} avg")
```

### Example 2: ETL Pipeline

```python
import vibesql
import csv

# Extract from CSV
db = vibesql.connect()
cursor = db.cursor()

cursor.execute("""
    CREATE TABLE raw_data (
        id INTEGER,
        name VARCHAR(100),
        value FLOAT,
        category VARCHAR(50)
    )
""")

with open('input.csv') as f:
    reader = csv.reader(f)
    next(reader)  # Skip header
    for row in reader:
        cursor.execute("INSERT INTO raw_data VALUES (?, ?, ?, ?)", tuple(row))

# Transform
cursor.execute("""
    CREATE TABLE summary AS
    SELECT
        category,
        COUNT(*) as count,
        AVG(value) as avg_value,
        MAX(value) as max_value
    FROM raw_data
    GROUP BY category
""")

# Load - save results
db.save("etl_output.sql")
```

### Example 3: Test Fixture

```python
import vibesql
import pytest

@pytest.fixture
def test_db():
    """Create a test database with sample data"""
    db = vibesql.connect()
    cursor = db.cursor()

    # Setup schema
    cursor.execute("""
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            username VARCHAR(50) UNIQUE,
            email VARCHAR(100)
        )
    """)

    # Insert test data
    cursor.execute("INSERT INTO users VALUES (1, 'alice', 'alice@example.com')")
    cursor.execute("INSERT INTO users VALUES (2, 'bob', 'bob@example.com')")

    yield db

    # Cleanup
    cursor.close()
    db.close()

def test_user_query(test_db):
    cursor = test_db.cursor()
    cursor.execute("SELECT username FROM users WHERE id = 1")
    result = cursor.fetchone()
    assert result[0] == 'alice'
```

### Example 4: Views

```python
import vibesql

db = vibesql.connect()
cursor = db.cursor()

# Create base table
cursor.execute("""
    CREATE TABLE orders (
        id INTEGER,
        customer VARCHAR(100),
        amount FLOAT,
        status VARCHAR(20)
    )
""")

cursor.execute("INSERT INTO orders VALUES (1, 'Alice', 150.00, 'completed')")
cursor.execute("INSERT INTO orders VALUES (2, 'Bob', 200.00, 'pending')")
cursor.execute("INSERT INTO orders VALUES (3, 'Alice', 75.00, 'completed')")

# Create a view
cursor.execute("""
    CREATE VIEW active_orders AS
    SELECT id, customer, amount
    FROM orders
    WHERE status = 'pending'
""")

# Query the view
cursor.execute("SELECT * FROM active_orders")
for row in cursor.fetchall():
    print(row)  # (2, 'Bob', 200.0)
```

### Example 5: Window Functions

```python
import vibesql

db = vibesql.connect()
cursor = db.cursor()

cursor.execute("""
    CREATE TABLE sales (
        employee VARCHAR(50),
        quarter VARCHAR(10),
        revenue FLOAT
    )
""")

cursor.execute("INSERT INTO sales VALUES ('Alice', 'Q1', 10000)")
cursor.execute("INSERT INTO sales VALUES ('Alice', 'Q2', 12000)")
cursor.execute("INSERT INTO sales VALUES ('Bob', 'Q1', 8000)")
cursor.execute("INSERT INTO sales VALUES ('Bob', 'Q2', 9500)")

# Use window functions
cursor.execute("""
    SELECT
        employee,
        quarter,
        revenue,
        LAG(revenue) OVER (PARTITION BY employee ORDER BY quarter) as prev_revenue,
        LEAD(revenue) OVER (PARTITION BY employee ORDER BY quarter) as next_revenue,
        FIRST_VALUE(revenue) OVER (PARTITION BY employee ORDER BY quarter) as first_revenue,
        LAST_VALUE(revenue) OVER (PARTITION BY employee ORDER BY quarter
                                  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_revenue
    FROM sales
    ORDER BY employee, quarter
""")

for row in cursor.fetchall():
    print(row)
```

---

## See Also

- [README.md](../README.md) - Project overview
- [Feature Status](reference/FEATURE_STATUS.md) - SQL feature breakdown
- [CLI Guide](CLI_GUIDE.md) - CLI documentation

---

## Testing

Run the test suite:

```bash
# Run Python binding tests
python3 crates/vibesql-python-bindings/tests/test_basic.py

# Run benchmarks
python3 benchmarks/python_overhead.py
```

---

## Troubleshooting

### Import Error

```python
ImportError: No module named 'vibesql'
```

**Solution:** Run `maturin develop` to build and install the module.

### Cache Not Working

If cache hit rates are unexpectedly low:

```python
# Check cache statistics
hits, misses, rate = cursor.cache_stats()
print(f"Hit rate: {rate:.2%}")

# Clear cache and retry
cursor.clear_cache()
```

### Memory Issues with Large Results

For large result sets, use `fetchmany()` instead of `fetchall()`:

```python
cursor.execute("SELECT * FROM huge_table")

# Bad: loads all rows into memory
# rows = cursor.fetchall()

# Good: process in batches
while True:
    batch = cursor.fetchmany(1000)
    if not batch:
        break
    process(batch)
```

---

**Last Updated:** November 2025
**Version:** 0.1.0
