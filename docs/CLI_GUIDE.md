# VibeSQL CLI User Guide

Complete guide to using the VibeSQL interactive command-line interface.

## Table of Contents

- [Getting Started](#getting-started)
- [Meta-Commands](#meta-commands)
- [Configuration File](#configuration-file)
- [Import and Export](#import-and-export)
- [Output Formats](#output-formats)
- [Database Persistence](#database-persistence)
- [Command-Line Arguments](#command-line-arguments)
- [Examples](#examples)

---

## Getting Started

### Installation

```bash
# Build from source
cargo build --release --bin vibesql

# Run the CLI
cargo run --bin vibesql
```

### Basic Usage

```bash
# Start interactive REPL
vibesql

# Load a database file
vibesql --database mydb.sql

# Execute a script file
vibesql --script queries.sql

# Set output format
vibesql --format json
```

### Quick Example

```sql
vibesql> CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100));
vibesql> INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');
vibesql> SELECT * FROM users;
+----+-------+
| id | name  |
+----+-------+
|  1 | Alice |
|  2 | Bob   |
+----+-------+
```

---

## Meta-Commands

Meta-commands are special commands that start with a backslash (`\`) and control the CLI behavior.

### Database Exploration

**`\d [table]`** - Describe table or list all tables
```sql
vibesql> \d
Tables:
  users
  orders
  products

vibesql> \d users
Table: users
Columns:
  id       INTEGER      NOT NULL PRIMARY KEY
  name     VARCHAR(100)
  email    VARCHAR(255)
```

**`\dt`** - List all tables
```sql
vibesql> \dt
Tables:
  employees
  departments
  projects
```

**`\ds`** - List all schemas
```sql
vibesql> \ds
Schemas:
  * PUBLIC (current)
    sales
    inventory
```

**`\di`** - List all indexes
```sql
vibesql> \di
Indexes:
  idx_users_email    | users    | (email)         | BTREE
  idx_orders_date    | orders   | (order_date)    | BTREE
  pk_users           | users    | (id)            | PRIMARY KEY
```

**`\du`** - List roles and users
```sql
vibesql> \du
Roles:
  * PUBLIC (current)
    admin
    readonly
```

### View Operations

Views allow you to save complex queries as virtual tables.

**CREATE VIEW** - Create a new view
```sql
vibesql> CREATE VIEW active_users AS
         SELECT id, name, email
         FROM users
         WHERE active = true;
View created: ACTIVE_USERS

vibesql> CREATE VIEW emp_summary (employee_id, employee_name) AS
         SELECT id, name
         FROM employees;
View created: EMP_SUMMARY
```

**DROP VIEW** - Remove a view
```sql
vibesql> DROP VIEW active_users;
View dropped: ACTIVE_USERS

vibesql> DROP VIEW IF EXISTS nonexistent_view;
View dropped (if existed)
```

**WITH CHECK OPTION** - Ensure view constraints
```sql
vibesql> CREATE VIEW recent_orders AS
         SELECT * FROM orders
         WHERE order_date >= '2024-01-01'
         WITH CHECK OPTION;
View created: RECENT_ORDERS
```

### Output Control

**`\f <format>`** - Set output format
```sql
vibesql> \f json
Output format set to: json

vibesql> SELECT * FROM users;
[
  {"id": 1, "name": "Alice"},
  {"id": 2, "name": "Bob"}
]
```

Supported formats:
- `table` - Formatted table (default)
- `json` - JSON array of objects
- `csv` - Comma-separated values

**`\timing`** - Toggle query execution timing
```sql
vibesql> \timing
Timing enabled.

vibesql> SELECT COUNT(*) FROM large_table;
+----------+
| COUNT(*) |
+----------+
| 1000000  |
+----------+
Time: 45.2 ms
```

### Help and Exit

**`\h`** or **`\help`** - Show help message
```sql
vibesql> \help
```

**`\q`** or **`\quit`** - Exit the CLI
```sql
vibesql> \quit
Goodbye!
```

---

## Configuration File

VibeSQL supports user preferences through a configuration file at `~/.vibesqlrc`.

### Configuration File Location

- **Default**: `~/.vibesqlrc`
- Format: TOML

### Configuration Options

Create `~/.vibesqlrc` with your preferences:

```toml
[display]
# Default output format: table, json, or csv
format = "table"

[database]
# Database to load automatically on startup
default_path = "/path/to/mydb.sql"

# Automatically save database when exiting
auto_save = true

[history]
# Location of command history file
file = "~/.vibesql_history"

# Maximum number of history entries to keep
max_entries = 10000

[query]
# Query timeout in seconds (0 = no timeout)
timeout_seconds = 30
```

### How Configuration Works

1. Configuration is loaded from `~/.vibesqlrc` on startup
2. Command-line arguments override config file settings
3. If the config file doesn't exist, defaults are used
4. Graceful fallback on parse errors

### Example Configurations

**Minimal Configuration** - Just change output format:
```toml
[display]
format = "json"
```

**Production Configuration** - Full settings:
```toml
[display]
format = "table"

[database]
default_path = "/Users/alice/databases/production.sql"
auto_save = true

[history]
file = "~/.vibesql_history"
max_entries = 10000

[query]
timeout_seconds = 60
```

**Developer Configuration** - Extended history, no timeout:
```toml
[display]
format = "json"

[history]
max_entries = 50000

[query]
timeout_seconds = 0  # No timeout for long-running queries
```

---

## Import and Export

### Export Data

**Export to CSV**:
```sql
vibesql> \copy employees TO '/tmp/employees.csv'
Exported 150 rows to /tmp/employees.csv
```

**Export to JSON**:
```sql
vibesql> \copy products TO '/tmp/products.json'
Exported 500 rows to /tmp/products.json
```

### Import Data

**Import from CSV**:
```sql
-- First create the table
vibesql> CREATE TABLE new_users (id INT, name VARCHAR(100), email VARCHAR(255));

-- Import data from CSV file
vibesql> \copy new_users FROM '/tmp/users.csv'
Imported 1000 rows into new_users
```

### File Format Details

**CSV Format**:
- First row contains column names (header)
- Comma-separated values
- Single quotes in values are automatically escaped
- NULL values represented as empty fields

Example CSV:
```csv
id,name,email
1,Alice,alice@example.com
2,Bob,bob@example.com
3,Charlie,charlie@example.com
```

**JSON Format**:
- Array of objects
- Each object represents one row
- Column names as object keys

Example JSON:
```json
[
  {"id": 1, "name": "Alice", "email": "alice@example.com"},
  {"id": 2, "name": "Bob", "email": "bob@example.com"},
  {"id": 3, "name": "Charlie", "email": "charlie@example.com"}
]
```

### Security Notes

- Table names are validated before export/import
- CSV values are properly escaped to prevent SQL injection
- Column names are validated against table schema
- Only valid table columns are accepted in CSV headers

---

## Output Formats

VibeSQL supports three output formats for query results.

### Table Format (Default)

Formatted ASCII table with borders:

```sql
vibesql> SELECT * FROM employees LIMIT 3;
+----+--------+------------+--------+
| id | name   | department | salary |
+----+--------+------------+--------+
|  1 | Alice  | Engineering| 95000  |
|  2 | Bob    | Sales      | 75000  |
|  3 | Carol  | Marketing  | 82000  |
+----+--------+------------+--------+
```

### JSON Format

Array of JSON objects:

```sql
vibesql> \f json
vibesql> SELECT * FROM employees LIMIT 2;
[
  {"id": 1, "name": "Alice", "department": "Engineering", "salary": 95000},
  {"id": 2, "name": "Bob", "department": "Sales", "salary": 75000}
]
```

### CSV Format

Comma-separated values with header:

```sql
vibesql> \f csv
vibesql> SELECT * FROM employees LIMIT 2;
id,name,department,salary
1,Alice,Engineering,95000
2,Bob,Sales,75000
```

---

## Database Persistence

### Save Database

**Save to default path** (if started with `--database`):
```sql
vibesql> \save
Database saved to: mydb.sql
```

**Save to specific file**:
```sql
vibesql> \save /tmp/backup.sql
Database saved to: /tmp/backup.sql
```

### Load Database

Load on startup:
```bash
vibesql --database mydb.sql
```

Or configure in `~/.vibesqlrc`:
```toml
[database]
default_path = "/path/to/mydb.sql"
```

### Auto-Save on Exit

Enable in configuration file:
```toml
[database]
auto_save = true
```

---

## Command-Line Arguments

```bash
vibesql [OPTIONS]

OPTIONS:
    --database <FILE>    Load database from SQL dump file
    --script <FILE>      Execute SQL script and exit
    --format <FORMAT>    Output format: table, json, csv (default: table)
    --help               Print help information
    --version            Print version information
```

### Examples

```bash
# Load database and start interactive session
vibesql --database production.sql

# Execute script with JSON output
vibesql --script queries.sql --format json

# Start with custom output format
vibesql --format csv

# Run script and save output
vibesql --script report.sql --format csv > report.csv
```

---

## Examples

### Data Analysis Workflow

```sql
-- 1. Load and explore data
vibesql --database sales.sql

-- 2. List tables
vibesql> \dt

-- 3. Describe table structure
vibesql> \d orders

-- 4. Run analysis query
vibesql> SELECT
    product_category,
    COUNT(*) as order_count,
    SUM(amount) as total_revenue
FROM orders
GROUP BY product_category
ORDER BY total_revenue DESC;

-- 5. Export results
vibesql> \f csv
vibesql> SELECT * FROM orders WHERE order_date >= '2025-01-01';
vibesql> \copy orders TO '/tmp/recent_orders.csv'

-- 6. Save database
vibesql> \save
```

### Data Migration

```sql
-- Export from source database
vibesql --database source.sql
vibesql> \copy users TO '/tmp/users.csv'
vibesql> \copy orders TO '/tmp/orders.csv'
vibesql> \quit

-- Import to new database
vibesql
vibesql> CREATE TABLE users (id INT, name VARCHAR(100), email VARCHAR(255));
vibesql> CREATE TABLE orders (id INT, user_id INT, amount DECIMAL(10,2));
vibesql> \copy users FROM '/tmp/users.csv'
vibesql> \copy orders FROM '/tmp/orders.csv'
vibesql> \save target.sql
```

### Reporting

```bash
# Generate daily report in CSV format
vibesql --database analytics.sql --script daily_report.sql --format csv > report.csv

# Email the report
cat report.csv | mail -s "Daily Sales Report" team@company.com
```

### Database Inspection

```sql
-- Connect and explore
vibesql --database unknown.sql

-- See what's in the database
vibesql> \dt               # List tables
vibesql> \ds               # List schemas
vibesql> \di               # List indexes
vibesql> \du               # List roles

-- Examine specific table
vibesql> \d employees

-- Sample data
vibesql> SELECT * FROM employees LIMIT 5;

-- Count records
vibesql> SELECT
    'employees' as table_name, COUNT(*) as row_count FROM employees
UNION ALL
    SELECT 'orders', COUNT(*) FROM orders
UNION ALL
    SELECT 'products', COUNT(*) FROM products;
```

---

## Tips and Best Practices

### Performance Tips

1. **Use timing** to identify slow queries:
   ```sql
   vibesql> \timing
   vibesql> SELECT * FROM large_table WHERE complex_condition;
   ```

2. **Check indexes** on large tables:
   ```sql
   vibesql> \di
   ```

3. **Export large results to CSV** instead of displaying:
   ```sql
   vibesql> \copy large_results TO '/tmp/results.csv'
   ```

### Workflow Tips

1. **Configure defaults** in `~/.vibesqlrc` for your common preferences

2. **Use history** - Up/down arrows to navigate previous commands

3. **Auto-save** enabled for data persistence:
   ```toml
   [database]
   auto_save = true
   ```

4. **Combine with shell tools**:
   ```bash
   # Count lines in CSV export
   vibesql --script export.sql --format csv | wc -l

   # Filter with grep
   vibesql --script all_users.sql --format csv | grep "@gmail.com"
   ```

---

## Troubleshooting

### Configuration Issues

**Config file not loading**:
- Check file location: `~/.vibesqlrc`
- Verify TOML syntax with a validator
- Check error messages on startup

**Auto-save not working**:
- Ensure `default_path` is set in config
- Or start with `--database` flag
- Check file permissions

### Import/Export Issues

**CSV import fails**:
- Verify CSV header matches table columns
- Check for special characters in data
- Ensure table exists before importing

**Permission errors**:
- Check read/write permissions on files
- Use absolute paths instead of relative paths

### Performance Issues

**Slow queries**:
- Enable `\timing` to measure execution time
- Check if indexes exist with `\di`
- Consider adding indexes for frequently queried columns

---

## See Also

- [README.md](../README.md) - Project overview and features
- [SQL:1999 Conformance](testing/SQL1999_CONFORMANCE.md) - Supported SQL features
