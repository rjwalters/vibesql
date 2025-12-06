# VibeSQL CLI Localization - English (US)
# This file contains all user-facing strings for the VibeSQL command-line interface.

# =============================================================================
# REPL Banner and Basic Messages
# =============================================================================

cli-banner = VibeSQL v{ $version } - SQL:1999 FULL Compliance Database
cli-help-hint = Type \help for help, \quit to exit
cli-goodbye = Goodbye!
locale-changed = Language changed to { $locale }

# =============================================================================
# Command Help Text (Clap Arguments)
# =============================================================================

cli-about = VibeSQL - SQL:1999 FULL Compliance Database

cli-long-about = VibeSQL command-line interface

    USAGE MODES:
      Interactive REPL:    vibesql (--database <FILE>)
      Execute Command:     vibesql -c "SELECT * FROM users"
      Execute File:        vibesql -f script.sql
      Execute from stdin:  cat data.sql | vibesql
      Generate Types:      vibesql codegen --schema schema.sql --output types.ts

    INTERACTIVE REPL:
      When started without -c, -f, or piped input, VibeSQL enters an interactive
      REPL with readline support, command history, and meta-commands like:
        \d (table)  - Describe table or list all tables
        \dt         - List tables
        \f <format> - Set output format
        \copy       - Import/export CSV/JSON
        \help       - Show all REPL commands

    SUBCOMMANDS:
      codegen           Generate TypeScript types from database schema

    CONFIGURATION:
      Settings can be configured in ~/.vibesqlrc (TOML format).
      Sections: display, database, history, query

    EXAMPLES:
      # Start interactive REPL with in-memory database
      vibesql

      # Use persistent database file
      vibesql --database mydata.db

      # Execute single command
      vibesql -c "CREATE TABLE users (id INT, name VARCHAR(100))"

      # Run SQL script file
      vibesql -f schema.sql -v

      # Import data from CSV
      echo "\copy users FROM 'data.csv'" | vibesql --database mydata.db

      # Export query results as JSON
      vibesql -d mydata.db -c "SELECT * FROM users" --format json

      # Generate TypeScript types from a schema file
      vibesql codegen --schema schema.sql --output src/types.ts

      # Generate TypeScript types from a running database
      vibesql codegen --database mydata.db --output src/types.ts

# Argument help strings
arg-database-help = Database file path (if not specified, uses in-memory database)
arg-file-help = Execute SQL commands from file
arg-command-help = Execute SQL command directly and exit
arg-stdin-help = Read SQL commands from stdin (auto-detected when piped)
arg-verbose-help = Show detailed output during file/stdin execution
arg-format-help = Output format for query results
arg-lang-help = Set the display language (e.g., en-US, es, ja)

# =============================================================================
# Codegen Subcommand
# =============================================================================

codegen-about = Generate TypeScript types from database schema

codegen-long-about = Generate TypeScript type definitions from a VibeSQL database schema.

    This command creates TypeScript interfaces for all tables in the database,
    along with metadata objects for runtime type checking and IDE support.

    INPUT SOURCES:
      --database <FILE>  Generate from an existing database file
      --schema <FILE>    Generate from a SQL schema file (CREATE TABLE statements)

    OUTPUT:
      --output <FILE>    Write generated types to this file (default: types.ts)

    OPTIONS:
      --camel-case       Convert column names to camelCase
      --no-metadata      Skip generating the tables metadata object

    EXAMPLES:
      # From a database file
      vibesql codegen --database mydata.db --output src/db/types.ts

      # From a SQL schema file
      vibesql codegen --schema schema.sql --output src/db/types.ts

      # With camelCase property names
      vibesql codegen --schema schema.sql --output types.ts --camel-case

codegen-schema-help = SQL schema file containing CREATE TABLE statements
codegen-output-help = Output file path for generated TypeScript
codegen-camel-case-help = Convert column names to camelCase
codegen-no-metadata-help = Skip generating table metadata object

codegen-from-schema = Generating TypeScript types from schema file: { $path }
codegen-from-database = Generating TypeScript types from database: { $path }
codegen-written = TypeScript types written to: { $path }
codegen-error-no-source = Either --database or --schema must be specified.
    Use 'vibesql codegen --help' for usage information.

# =============================================================================
# Meta-commands Help (\help output)
# =============================================================================

help-title = Meta-commands:
help-describe = \d (table)      - Describe table or list all tables
help-tables = \dt             - List tables
help-schemas = \ds             - List schemas
help-indexes = \di             - List indexes
help-roles = \du             - List roles/users
help-format = \f <format>     - Set output format (table, json, csv, markdown, html)
help-timing = \timing         - Toggle query timing
help-copy-to = \copy <table> TO <file>   - Export table to CSV/JSON file
help-copy-from = \copy <table> FROM <file> - Import CSV file into table
help-save = \save (file)    - Save database to SQL dump file
help-errors = \errors         - Show recent error history
help-help = \h, \help      - Show this help
help-quit = \q, \quit      - Exit

help-sql-title = SQL Introspection:
help-show-tables = SHOW TABLES                  - List all tables
help-show-databases = SHOW DATABASES               - List all schemas/databases
help-show-columns = SHOW COLUMNS FROM <table>    - Show table columns
help-show-index = SHOW INDEX FROM <table>      - Show table indexes
help-show-create = SHOW CREATE TABLE <table>    - Show CREATE TABLE statement
help-describe-sql = DESCRIBE <table>             - Alias for SHOW COLUMNS

help-examples-title = Examples:
help-example-create = CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100));
help-example-insert = INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');
help-example-select = SELECT * FROM users;
help-example-show-tables = SHOW TABLES;
help-example-show-columns = SHOW COLUMNS FROM users;
help-example-describe = DESCRIBE users;
help-example-format-json = \f json
help-example-format-md = \f markdown
help-example-copy-to = \copy users TO '/tmp/users.csv'
help-example-copy-from = \copy users FROM '/tmp/users.csv'
help-example-copy-json = \copy users TO '/tmp/users.json'
help-example-errors = \errors

# =============================================================================
# Status Messages
# =============================================================================

format-changed = Output format set to: { $format }
database-saved = Database saved to: { $path }
no-database-file = Error: No database file specified. Use \save <filename> or start with --database flag

# =============================================================================
# Error Display
# =============================================================================

no-errors = No errors in this session.
recent-errors = Recent errors:

# =============================================================================
# Script Execution Messages
# =============================================================================

script-no-statements = No SQL statements found in script
script-executing = Executing statement { $current } of { $total }...
script-error = Error executing statement { $index }: { $error }
script-summary-title = === Script Execution Summary ===
script-total = Total statements: { $count }
script-successful = Successful: { $count }
script-failed = Failed: { $count }
script-failed-error = { $count } statements failed

# =============================================================================
# Output Formatting
# =============================================================================

rows-with-time = { $count } rows in set ({ $time }s)
rows-count = { $count } rows

# =============================================================================
# Warnings
# =============================================================================

warning-config-load = Warning: Could not load config file: { $error }
warning-auto-save-failed = Warning: Failed to auto-save database: { $error }
warning-save-on-exit-failed = Warning: Failed to save database on exit: { $error }

# =============================================================================
# File Operations
# =============================================================================

file-read-error = Failed to read file '{ $path }': { $error }
stdin-read-error = Failed to read from stdin: { $error }
database-load-error = Failed to load database: { $error }
