#!/usr/bin/env python3
"""
VibeSQL database helper module for dogfooding.

This module provides a consistent interface for all benchmark scripts to use
VibeSQL as the storage engine instead of SQLite. This enables us to dogfood
our own database for performance tracking.

Usage:
    from vibesql_db import get_connection, save_connection, get_db_path

    db, cursor = get_connection()
    cursor.execute("SELECT * FROM benchmark_runs")
    results = cursor.fetchall()
    save_connection(db)
"""

import os
import sys
from pathlib import Path
from typing import Tuple, Optional, Any

# Try to import vibesql, fall back to building instructions
try:
    import vibesql
except ImportError:
    print("❌ vibesql Python bindings not installed")
    print("")
    print("To build and install:")
    print("  make build-python")
    print("  pip install target/wheels/vibesql-*.whl")
    print("")
    sys.exit(1)


def get_db_path() -> Path:
    """Get the path to the benchmark results database (VibeSQL format)."""
    return Path.home() / ".vibesql" / "test_results" / "benchmark_results.vbsql"


def get_legacy_db_path() -> Path:
    """Get the path to the legacy SQLite database (for migration)."""
    return Path.home() / ".vibesql" / "test_results" / "benchmark_results.db"


def get_connection() -> Tuple[Any, Any]:
    """
    Get a database connection and cursor.

    Loads the existing database if it exists, otherwise creates a new one.

    Returns:
        Tuple of (database, cursor)
    """
    db_path = get_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    if db_path.exists():
        try:
            db = vibesql.Database.load(str(db_path))
        except Exception as e:
            print(f"⚠️  Failed to load existing database: {e}")
            print("   Creating new database...")
            db = vibesql.connect()
    else:
        db = vibesql.connect()

    cursor = db.cursor()
    return db, cursor


def save_connection(db: Any) -> None:
    """
    Save the database to disk.

    Args:
        db: The database connection to save
    """
    db_path = get_db_path()
    db.save(str(db_path))


def get_last_insert_id(cursor: Any, table: str, id_column: str = "run_id") -> int:
    """
    Get the last inserted ID for a table.

    Since VibeSQL doesn't support last_insert_rowid(), we query for MAX(id).
    This is safe because we're the only writer and inserts are sequential.

    Args:
        cursor: Database cursor
        table: Table name
        id_column: Name of the ID column (default: "run_id")

    Returns:
        The maximum ID value in the table
    """
    cursor.execute(f"SELECT MAX({id_column}) FROM {table}")
    result = cursor.fetchone()
    return result[0] if result and result[0] is not None else 0


def init_schema(cursor: Any, schema_sql: str) -> None:
    """
    Initialize the database schema.

    Executes schema SQL statements one at a time.
    Handles VibeSQL-specific syntax requirements.

    Args:
        cursor: Database cursor
        schema_sql: SQL schema definition
    """
    # Split schema into individual statements
    statements = []
    current_stmt = []

    for line in schema_sql.split('\n'):
        # Skip comments and empty lines
        stripped = line.strip()
        if stripped.startswith('--') or not stripped:
            continue

        current_stmt.append(line)

        # Check if this line ends a statement
        if stripped.endswith(';'):
            statements.append('\n'.join(current_stmt))
            current_stmt = []

    # Execute each statement
    for stmt in statements:
        stmt = stmt.strip()
        if stmt:
            # VibeSQL doesn't support IF NOT EXISTS - strip it and handle errors
            stmt = stmt.replace('IF NOT EXISTS ', '')
            stmt = stmt.replace('if not exists ', '')

            # Skip CREATE INDEX statements - VibeSQL doesn't support them yet
            # (indexes are created automatically via PRIMARY KEY and UNIQUE constraints)
            stmt_upper = stmt.upper()
            if stmt_upper.startswith('CREATE INDEX') or stmt_upper.startswith('CREATE UNIQUE INDEX'):
                continue

            # Skip CREATE VIEW statements - VibeSQL doesn't support them yet
            # We can query the base tables directly
            if stmt_upper.startswith('CREATE VIEW'):
                continue

            try:
                cursor.execute(stmt)
            except Exception as e:
                error_str = str(e).lower()
                # Ignore "already exists" errors (various forms)
                if "already exists" in error_str:
                    continue
                if "alreadyexists" in error_str:
                    continue
                # Ignore errors for tables/indexes that already exist
                if "duplicate" in error_str:
                    continue
                # Ignore unsupported feature errors for VIEWs with window functions
                if "not yet supported" in error_str:
                    continue
                # Also catch the specific TableAlreadyExists error
                if "tablealreadyexists" in error_str.replace(" ", ""):
                    continue
                raise


def format_value(value: Any) -> str:
    """
    Format a Python value for SQL insertion.

    Args:
        value: The value to format

    Returns:
        SQL-safe string representation
    """
    if value is None:
        return "NULL"
    elif isinstance(value, str):
        # Escape single quotes
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
    elif isinstance(value, (int, float)):
        return str(value)
    else:
        return f"'{str(value)}'"


def quote_identifier(name: str) -> str:
    """
    Quote a SQL identifier (table/column name) to handle reserved keywords.

    VibeSQL uses double quotes for identifier quoting (SQL standard).

    Args:
        name: The identifier name

    Returns:
        Quoted identifier
    """
    # Escape any existing double quotes
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def execute_insert(cursor: Any, table: str, columns: list, values: list, debug: bool = False) -> None:
    """
    Execute an INSERT statement with proper value formatting.

    Args:
        cursor: Database cursor
        table: Table name
        columns: List of column names
        values: List of values (in same order as columns)
        debug: If True, print the SQL before executing
    """
    # Don't quote column names - VibeSQL doesn't support quoted identifiers in INSERT
    # Use non-reserved column names instead (e.g., 'run_timestamp' instead of 'timestamp')
    cols_str = ", ".join(columns)
    vals_str = ", ".join(format_value(v) for v in values)
    sql = f"INSERT INTO {table} ({cols_str}) VALUES ({vals_str})"
    if debug or os.environ.get("VIBESQL_DEBUG"):
        print(f"DEBUG SQL: {sql}")
    cursor.execute(sql)


# Make this module runnable for testing
if __name__ == "__main__":
    print("Testing vibesql_db module...")

    db, cursor = get_connection()

    # Test basic query
    cursor.execute("SELECT 1 as test")
    result = cursor.fetchone()
    print(f"✓ Basic query: {result}")

    # Test schema initialization (create a test table)
    test_schema = """
    CREATE TABLE _test_table (
        id INTEGER PRIMARY KEY,
        name TEXT
    );
    """
    init_schema(cursor, test_schema)
    print("✓ Schema initialization")

    # Test insert
    execute_insert(cursor, "_test_table", ["id", "name"], [1, "test"])
    print("✓ Insert")

    # Test select
    cursor.execute("SELECT * FROM _test_table")
    result = cursor.fetchall()
    print(f"✓ Select: {result}")

    # Clean up test table
    cursor.execute("DROP TABLE _test_table")

    # Save database
    save_connection(db)
    print(f"✓ Database saved to {get_db_path()}")

    print("\nAll tests passed!")
