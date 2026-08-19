# VibeSQL CLI

Command-line interface for VibeSQL, providing interactive SQL querying and database management.

## Features

### Phase 1: Basic REPL
- Interactive SQL prompt with line editing and history
- Execute SQL statements and display results
- Basic table formatting for query results
- Command history and error handling
- Meta-commands: `\help`, `\q`, `\d`, `\dt`, `\timing`

### Phase 2: Script Execution & File I/O (Current)
- Execute SQL from files: `vibesql -f script.sql`
- Execute SQL from stdin: `cat queries.sql | vibesql` or `vibesql --stdin`
- Execute single commands: `vibesql -c "SELECT * FROM users"`
- Support multiple statements in scripts
- Auto-detection of piped input
- Verbose output mode: `vibesql -f script.sql -v`
- Execution summary with success/failure counts
- Data import/export utilities (CSV, JSON)

### Phase 3: Advanced Features (Current)
- More meta-commands: `\ds` (schemas), `\di` (indexes), `\du` (roles/users)
- Output format control: `\f <format>` or `--format` flag
- Output format options (table, CSV, JSON)
- Query timing and performance statistics
- Automatic output format switching in REPL

### Phase 4: Database Management (Future)
- Connection to persistent database files
- Schema introspection enhancements
- Database backup/restore
- Query profiling
- Configuration file support (.vibesqlrc)
- Syntax highlighting
- Multi-line statement editing

## Usage

### Interactive REPL

```bash
# Start interactive session
cargo run --bin vibesql

# Start with specific database
cargo run --bin vibesql -- --database mydb.vsql
```

### Command Execution

```bash
# Execute single SQL command
cargo run --bin vibesql -- -c "SELECT 1"

# Execute single SQL command with verbose output
cargo run --bin vibesql -- -c "SELECT 1" -v
```

### File Execution

```bash
# Execute SQL from file
cargo run --bin vibesql -- -f script.sql

# Execute SQL from file with verbose output and summary
cargo run --bin vibesql -- -f script.sql --verbose

# Execute with specific output format
cargo run --bin vibesql -- -f script.sql --format json
cargo run --bin vibesql -- -f script.sql --format csv
```

### Stdin Execution

```bash
# Execute from stdin (auto-detected when piped)
cat queries.sql | cargo run --bin vibesql

# Or explicitly request stdin
cargo run --bin vibesql -- --stdin < queries.sql

# Pipe from other commands with specific format
echo "SELECT 1; SELECT 2;" | cargo run --bin vibesql -- --format json
```

### Output Formats

```bash
# Table format (default, ASCII tables)
cargo run --bin vibesql -- -c "SELECT * FROM users" --format table

# JSON format (array of objects)
cargo run --bin vibesql -- -c "SELECT * FROM users" --format json

# CSV format (comma-separated values)
cargo run --bin vibesql -- -c "SELECT * FROM users" --format csv
```

## Meta-Commands

```
\d [table]      - Describe table or list all tables
\dt             - List tables
\ds             - List schemas
\di             - List indexes
\du             - List roles/users
\f <format>     - Set output format (table, json, csv)
\timing         - Toggle query timing
\h, \help       - Show help
\q, \quit       - Exit
```

### Format Command Examples

```
vibesql> \f json
Output format set to: json

vibesql> SELECT * FROM users;
[
  {"id": "1", "name": "Alice"},
  {"id": "2", "name": "Bob"}
]

vibesql> \f csv
Output format set to: csv

vibesql> SELECT * FROM users;
id,name
1,Alice
2,Bob

vibesql> \f table
Output format set to: table
```

## Example Session

```
$ vibesql
VibeSQL v0.1.0 - SQL:1999 FULL Compliance Database
Type \help for help, \quit to exit

vibesql> CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100));
Table created successfully

vibesql> INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');
2 rows inserted

vibesql> SELECT * FROM users;
+----+-------+
| id | name  |
+----+-------+
| 1  | Alice |
| 2  | Bob   |
+----+-------+
2 rows

vibesql> \d users
Table: users
Columns:
  id   INT PRIMARY KEY
  name VARCHAR(100)

vibesql> \quit
Goodbye!
```

## Architecture

The CLI is organized into modules:

- `main.rs` - Entry point, CLI argument parsing, and mode selection
- `repl.rs` - Interactive REPL implementation
- `executor.rs` - SQL execution wrapper (SELECT, CREATE, INSERT, UPDATE, DELETE)
- `formatter.rs` - Result formatting (table, CSV, JSON)
- `commands.rs` - Meta-command parsing and handling
- `script.rs` - Batch SQL execution from files and stdin
- `data_io.rs` - Data import/export utilities (CSV, JSON)
- `error.rs` - CLI-specific error types

## Dependencies

- `clap 4` - Command-line argument parsing
- `rustyline 18` - Line editing and history
- `prettytable-rs 0.10` - Table formatting
- `serde_json 1.0` - JSON formatting
- `anyhow 1.0` - Error handling
- `toml 1.1` - Config file (`~/.vibesqlrc`) parsing
- `dirs 6.0` - Home/config directory resolution
- `regex 1.10` - Pattern matching
- `rusqlite` - SQLite comparison/interop
- `log 0.4` - Logging
