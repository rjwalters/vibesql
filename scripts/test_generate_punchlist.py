#!/usr/bin/env python3
"""
Test suite for generate_punchlist.py with VibeSQL integration.

Tests the core functionality of storing SQLLogicTest results in VibeSQL
and exporting SQL dumps.
"""

import unittest
import tempfile
import os
from pathlib import Path

# Shared bindings guard (scripts/conftest.py). scripts/ is on sys.path both
# under pytest (rootdir insertion) and when this file is run directly.
from conftest import MissingBindingsError, bindings_required, load_vibesql


class TestVibesqlPunchlist(unittest.TestCase):
    """Test VibeSQL integration for punchlist generation."""

    def setUp(self):
        """Set up test fixtures."""
        try:
            # Verifies the installed bindings match this checkout's version.
            # A STALE wheel raises StaleBindingsError, which intentionally
            # propagates as a test FAILURE (never a skip) — testing a stale
            # wheel yields misleading engine-bug failures (issue #6323).
            self.vibesql = load_vibesql()
        except MissingBindingsError:
            if bindings_required():
                raise  # VIBESQL_REQUIRE_BINDINGS=1: absence is a failure
            self.skipTest(
                "vibesql Python bindings not installed in this environment "
                "(build them with `make test-scripts` or `make build-python`)"
            )

        # Create temporary directory for test files
        self.test_dir = tempfile.mkdtemp()
        self.schema_file = Path(__file__).parent / "schema" / "test_results.sql"

    def tearDown(self):
        """Clean up test files."""
        import shutil
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_create_database_from_schema(self):
        """Test that we can create a database and load the schema."""
        db = self.vibesql.connect()
        cursor = db.cursor()

        # Load schema
        with open(self.schema_file, 'r') as f:
            schema_sql = f.read()

        # Execute each CREATE TABLE statement. The schema file prefixes every
        # statement with `--` comment banners, so comment-only lines must be
        # stripped before the CREATE TABLE prefix check or every statement is
        # silently filtered out.
        for statement in schema_sql.split(';'):
            lines = [
                line for line in statement.splitlines()
                if not line.strip().startswith('--')
            ]
            statement = '\n'.join(lines).strip()
            if statement and statement.upper().startswith('CREATE TABLE'):
                cursor.execute(statement)

        # Verify tables exist by inserting test data
        cursor.execute("""
            INSERT INTO test_files (file_path, category, status)
            VALUES ('test.sql', 'index', 'PASS')
        """)

        cursor.execute("SELECT COUNT(*) FROM test_files")
        count = cursor.fetchone()[0]
        self.assertEqual(count, 1)

        db.close()

    # KNOWN BUG (#6359): the bindings' statement cache replays the FIRST
    # execution's parameter values for repeated identical parameterized SQL,
    # so the second insert spuriously fails with "UNIQUE constraint failed".
    # When #6359 is fixed this test will report an UNEXPECTED SUCCESS —
    # remove the decorator then.
    @unittest.expectedFailure
    def test_insert_test_results(self):
        """Test inserting test file records."""
        db = self.vibesql.connect()
        cursor = db.cursor()

        # Create schema
        cursor.execute("""
            CREATE TABLE test_files (
                file_path VARCHAR(500) PRIMARY KEY,
                category VARCHAR(50) NOT NULL,
                subcategory VARCHAR(50),
                status VARCHAR(20) NOT NULL,
                last_tested TIMESTAMP,
                last_passed TIMESTAMP
            )
        """)

        # Insert test data
        test_files = [
            ('index/between/1/slt_good_0.test', 'index', 'between', 'PASS'),
            ('index/delete/10/slt_good_0.test', 'index', 'delete', 'FAIL'),
            ('random/select/1000/slt_good_0.test', 'random', 'select', 'UNTESTED'),
        ]

        for file_path, category, subcategory, status in test_files:
            cursor.execute("""
                INSERT INTO test_files (file_path, category, subcategory, status)
                VALUES (?, ?, ?, ?)
            """, (file_path, category, subcategory, status))

        # Verify data
        cursor.execute("SELECT COUNT(*) FROM test_files")
        count = cursor.fetchone()[0]
        self.assertEqual(count, 3)

        cursor.execute("SELECT COUNT(*) FROM test_files WHERE status='PASS'")
        passed = cursor.fetchone()[0]
        self.assertEqual(passed, 1)

        db.close()

    def test_save_database(self):
        """Test persisting a database to disk with Database.save().

        The bindings' former SQL-dump export (`save_sql_dump`) no longer
        exists; `save(path)` writes a binary snapshot (preserves sequences
        and column defaults). This test covers the export half of the
        punchlist persistence round-trip.
        """
        db = self.vibesql.connect()
        cursor = db.cursor()

        # Create simple table
        cursor.execute("""
            CREATE TABLE test_files (
                file_path VARCHAR(500) PRIMARY KEY,
                category VARCHAR(50) NOT NULL,
                status VARCHAR(20) NOT NULL
            )
        """)

        # Insert data
        cursor.execute("""
            INSERT INTO test_files (file_path, category, status)
            VALUES ('test.sql', 'index', 'PASS')
        """)

        # Save binary snapshot
        db_file = os.path.join(self.test_dir, 'test_db.vbsql')
        db.save(db_file)

        # Verify file exists and is non-empty
        self.assertTrue(os.path.exists(db_file))
        self.assertGreater(os.path.getsize(db_file), 0)

        db.close()

    def test_load_existing_database(self):
        """Test loading a previously saved database with Database.load()."""
        # Create and save database
        db1 = self.vibesql.connect()
        cursor1 = db1.cursor()

        cursor1.execute("""
            CREATE TABLE test_files (
                file_path VARCHAR(500) PRIMARY KEY,
                status VARCHAR(20) NOT NULL
            )
        """)
        cursor1.execute("""
            INSERT INTO test_files (file_path, status) VALUES ('test1.sql', 'PASS')
        """)
        cursor1.execute("""
            INSERT INTO test_files (file_path, status) VALUES ('test2.sql', 'FAIL')
        """)

        db_file = os.path.join(self.test_dir, 'test_db.vbsql')
        db1.save(db_file)
        db1.close()

        # Load into new database instance
        db2 = self.vibesql.Database.load(db_file)
        cursor2 = db2.cursor()

        # Verify data loaded
        cursor2.execute("SELECT COUNT(*) FROM test_files")
        count = cursor2.fetchone()[0]
        self.assertEqual(count, 2)

        cursor2.execute("SELECT file_path FROM test_files ORDER BY file_path")
        rows = cursor2.fetchall()
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0][0], 'test1.sql')
        self.assertEqual(rows[1][0], 'test2.sql')

        db2.close()

    # KNOWN BUG (#6359): repeated identical parameterized INSERTs replay the
    # first execution's values (statement-cache bug), so this fails with a
    # spurious "UNIQUE constraint failed". Remove the decorator when #6359 is
    # fixed (this test will report an UNEXPECTED SUCCESS then).
    @unittest.expectedFailure
    def test_summary_queries_match_old_format(self):
        """Test that SQL queries produce same stats as old JSON format."""
        db = self.vibesql.connect()
        cursor = db.cursor()

        # Create schema
        cursor.execute("""
            CREATE TABLE test_files (
                file_path VARCHAR(500) PRIMARY KEY,
                category VARCHAR(50) NOT NULL,
                status VARCHAR(20) NOT NULL
            )
        """)

        # Insert test data matching known distribution
        test_data = [
            ('index', 'PASS', 75),
            ('index', 'FAIL', 132),
            ('index', 'UNTESTED', 7),
            ('evidence', 'PASS', 6),
            ('evidence', 'FAIL', 6),
            ('random', 'PASS', 2),
            ('random', 'FAIL', 386),
            ('random', 'UNTESTED', 3),
        ]

        file_id = 1
        for category, status, count in test_data:
            for _ in range(count):
                cursor.execute("""
                    INSERT INTO test_files (file_path, category, status)
                    VALUES (?, ?, ?)
                """, (f'{category}/test_{file_id}.sql', category, status))
                file_id += 1

        # Query overall stats
        cursor.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN status='PASS' THEN 1 ELSE 0 END) as passed,
                SUM(CASE WHEN status='FAIL' THEN 1 ELSE 0 END) as failed,
                SUM(CASE WHEN status='UNTESTED' THEN 1 ELSE 0 END) as untested
            FROM test_files
        """)

        row = cursor.fetchone()
        total, passed, failed, untested = row

        # Expected from old JSON format
        self.assertEqual(total, 617)  # Sum of all counts
        self.assertEqual(passed, 83)
        self.assertEqual(failed, 524)
        self.assertEqual(untested, 10)

        db.close()


if __name__ == '__main__':
    unittest.main()
