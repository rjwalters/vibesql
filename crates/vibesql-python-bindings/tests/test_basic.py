"""
Basic tests for vibesql Python bindings
"""
import vibesql


def test_connection():
    """Test basic connection creation"""
    db = vibesql.connect()
    assert db is not None
    db.close()


def test_cursor_creation():
    """Test cursor creation from database"""
    db = vibesql.connect()
    cursor = db.cursor()
    assert cursor is not None
    cursor.close()
    db.close()


def test_create_table():
    """Test CREATE TABLE statement"""
    db = vibesql.connect()
    cursor = db.cursor()
    cursor.execute("CREATE TABLE users (id INTEGER, name VARCHAR(50))")
    cursor.close()
    db.close()


def test_insert_select():
    """Test INSERT and SELECT operations"""
    db = vibesql.connect()
    cursor = db.cursor()
    cursor.execute("CREATE TABLE users (id INTEGER, name VARCHAR(50))")
    cursor.execute("INSERT INTO users VALUES (1, 'Alice')")
    cursor.execute("SELECT * FROM users")
    rows = cursor.fetchall()
    assert len(rows) == 1
    assert rows[0] == (1, 'Alice')
    cursor.close()
    db.close()


def test_multiple_inserts():
    """Test multiple INSERT operations"""
    db = vibesql.connect()
    cursor = db.cursor()
    cursor.execute("CREATE TABLE users (id INTEGER, name VARCHAR(50))")
    cursor.execute("INSERT INTO users VALUES (1, 'Alice')")
    cursor.execute("INSERT INTO users VALUES (2, 'Bob')")
    cursor.execute("INSERT INTO users VALUES (3, 'Charlie')")
    cursor.execute("SELECT * FROM users")
    rows = cursor.fetchall()
    assert len(rows) == 3
    assert rows[0] == (1, 'Alice')
    assert rows[1] == (2, 'Bob')
    assert rows[2] == (3, 'Charlie')
    cursor.close()
    db.close()


def test_data_types():
    """Test various SQL data types"""
    db = vibesql.connect()
    cursor = db.cursor()
    # Create a dummy table for testing various data types
    cursor.execute("CREATE TABLE datatypes (i INTEGER, f FLOAT, s VARCHAR(50), b BOOLEAN, n INTEGER)")
    cursor.execute("INSERT INTO datatypes VALUES (42, 3.14, 'hello', TRUE, NULL)")
    cursor.execute("SELECT * FROM datatypes")
    row = cursor.fetchone()
    assert row[0] == 42
    assert abs(row[1] - 3.14) < 0.01  # Float comparison with tolerance
    assert row[2] == 'hello'
    assert row[3] == True
    assert row[4] is None
    cursor.close()
    db.close()


def test_fetchone():
    """Test fetchone() method"""
    db = vibesql.connect()
    cursor = db.cursor()
    cursor.execute("CREATE TABLE numbers (n INTEGER)")
    cursor.execute("INSERT INTO numbers VALUES (1)")
    cursor.execute("INSERT INTO numbers VALUES (2)")
    cursor.execute("INSERT INTO numbers VALUES (3)")
    cursor.execute("SELECT * FROM numbers")

    row1 = cursor.fetchone()
    assert row1 == (1,)

    row2 = cursor.fetchone()
    assert row2 == (2,)

    row3 = cursor.fetchone()
    assert row3 == (3,)

    row4 = cursor.fetchone()
    assert row4 is None

    cursor.close()
    db.close()


def test_fetchmany():
    """Test fetchmany() method"""
    db = vibesql.connect()
    cursor = db.cursor()
    cursor.execute("CREATE TABLE numbers (n INTEGER)")
    for i in range(10):
        cursor.execute(f"INSERT INTO numbers VALUES ({i})")

    cursor.execute("SELECT * FROM numbers")

    rows = cursor.fetchmany(3)
    assert len(rows) == 3
    assert rows[0] == (0,)
    assert rows[1] == (1,)
    assert rows[2] == (2,)

    rows = cursor.fetchmany(5)
    assert len(rows) == 5

    cursor.close()
    db.close()


def test_update():
    """Test UPDATE statement"""
    db = vibesql.connect()
    cursor = db.cursor()
    cursor.execute("CREATE TABLE users (id INTEGER, name VARCHAR(50))")
    cursor.execute("INSERT INTO users VALUES (1, 'Alice')")
    cursor.execute("UPDATE users SET name = 'Alicia' WHERE id = 1")
    cursor.execute("SELECT * FROM users")
    row = cursor.fetchone()
    assert row == (1, 'Alicia')
    cursor.close()
    db.close()


def test_delete():
    """Test DELETE statement"""
    db = vibesql.connect()
    cursor = db.cursor()
    cursor.execute("CREATE TABLE users (id INTEGER, name VARCHAR(50))")
    cursor.execute("INSERT INTO users VALUES (1, 'Alice')")
    cursor.execute("INSERT INTO users VALUES (2, 'Bob')")
    cursor.execute("DELETE FROM users WHERE id = 1")
    cursor.execute("SELECT * FROM users")
    rows = cursor.fetchall()
    assert len(rows) == 1
    assert rows[0] == (2, 'Bob')
    cursor.close()
    db.close()


def test_rowcount():
    """Test rowcount attribute"""
    db = vibesql.connect()
    cursor = db.cursor()
    cursor.execute("CREATE TABLE users (id INTEGER, name VARCHAR(50))")
    cursor.execute("INSERT INTO users VALUES (1, 'Alice')")
    assert cursor.rowcount == 1

    cursor.execute("INSERT INTO users VALUES (2, 'Bob')")
    assert cursor.rowcount == 1

    cursor.execute("SELECT * FROM users")
    cursor.fetchall()
    assert cursor.rowcount == 2

    cursor.close()
    db.close()


def test_drop_table():
    """Test DROP TABLE statement"""
    db = vibesql.connect()
    cursor = db.cursor()
    cursor.execute("CREATE TABLE temp (id INTEGER)")
    cursor.execute("DROP TABLE temp")
    cursor.close()
    db.close()


def test_multiple_cursors():
    """Test multiple cursors on same database"""
    db = vibesql.connect()

    cursor1 = db.cursor()
    cursor2 = db.cursor()

    cursor1.execute("CREATE TABLE users (id INTEGER, name VARCHAR(50))")
    cursor1.execute("INSERT INTO users VALUES (1, 'Alice')")

    cursor2.execute("SELECT * FROM users")
    rows = cursor2.fetchall()
    assert len(rows) == 1
    assert rows[0] == (1, 'Alice')

    cursor1.close()
    cursor2.close()
    db.close()


# ============================================================================
# Parameterized Query Tests (Issue #824)
# ============================================================================

def test_parameterized_select():
    """Test basic parameterized SELECT with single parameter"""
    db = vibesql.connect()
    cursor = db.cursor()
    cursor.execute("CREATE TABLE users (id INTEGER, name VARCHAR(50))")
    cursor.execute("INSERT INTO users VALUES (1, 'Alice')")
    cursor.execute("INSERT INTO users VALUES (2, 'Bob')")
    cursor.execute("INSERT INTO users VALUES (3, 'Charlie')")

    # Parameterized SELECT
    cursor.execute("SELECT * FROM users WHERE id = ?", (2,))
    rows = cursor.fetchall()
    assert len(rows) == 1
    assert rows[0] == (2, 'Bob')

    cursor.close()
    db.close()


def test_parameterized_insert():
    """Test parameterized INSERT statement"""
    db = vibesql.connect()
    cursor = db.cursor()
    cursor.execute("CREATE TABLE users (id INTEGER, name VARCHAR(50))")

    # Parameterized INSERT
    cursor.execute("INSERT INTO users VALUES (?, ?)", (1, 'Alice'))
    cursor.execute("SELECT * FROM users")
    rows = cursor.fetchall()
    assert len(rows) == 1
    assert rows[0] == (1, 'Alice')

    cursor.close()
    db.close()


def test_parameterized_update():
    """Test parameterized UPDATE statement"""
    db = vibesql.connect()
    cursor = db.cursor()
    cursor.execute("CREATE TABLE users (id INTEGER, name VARCHAR(50))")
    cursor.execute("INSERT INTO users VALUES (1, 'Alice')")
    cursor.execute("INSERT INTO users VALUES (2, 'Bob')")

    # Parameterized UPDATE
    cursor.execute("UPDATE users SET name = ? WHERE id = ?", ('Alicia', 1))
    cursor.execute("SELECT * FROM users WHERE id = 1")
    row = cursor.fetchone()
    assert row == (1, 'Alicia')

    cursor.close()
    db.close()


def test_parameterized_delete():
    """Test parameterized DELETE statement"""
    db = vibesql.connect()
    cursor = db.cursor()
    cursor.execute("CREATE TABLE users (id INTEGER, name VARCHAR(50))")
    cursor.execute("INSERT INTO users VALUES (1, 'Alice')")
    cursor.execute("INSERT INTO users VALUES (2, 'Bob')")
    cursor.execute("INSERT INTO users VALUES (3, 'Charlie')")

    # Parameterized DELETE
    cursor.execute("DELETE FROM users WHERE id = ?", (2,))
    cursor.execute("SELECT * FROM users")
    rows = cursor.fetchall()
    assert len(rows) == 2
    assert rows[0] == (1, 'Alice')
    assert rows[1] == (3, 'Charlie')

    cursor.close()
    db.close()


def test_parameterized_multiple_types():
    """Test parameterized query with multiple parameter types"""
    db = vibesql.connect()
    cursor = db.cursor()
    cursor.execute("CREATE TABLE data (id INTEGER, value FLOAT, name VARCHAR(50), active BOOLEAN)")

    # Insert with multiple types
    cursor.execute("INSERT INTO data VALUES (?, ?, ?, ?)", (1, 3.14, 'test', True))
    cursor.execute("SELECT * FROM data")
    row = cursor.fetchone()
    assert row[0] == 1
    assert abs(row[1] - 3.14) < 0.01
    assert row[2] == 'test'
    assert row[3] == True

    cursor.close()
    db.close()


def test_parameterized_null_value():
    """Test parameterized query with NULL parameter"""
    db = vibesql.connect()
    cursor = db.cursor()
    cursor.execute("CREATE TABLE users (id INTEGER, name VARCHAR(50))")

    # Insert with NULL
    cursor.execute("INSERT INTO users VALUES (?, ?)", (1, None))
    cursor.execute("SELECT * FROM users")
    row = cursor.fetchone()
    assert row[0] == 1
    assert row[1] is None

    cursor.close()
    db.close()


def test_parameterized_string_with_quotes():
    """Test parameterized query with string containing quotes"""
    db = vibesql.connect()
    cursor = db.cursor()
    cursor.execute("CREATE TABLE users (id INTEGER, name VARCHAR(50))")

    # Insert string with single quotes
    cursor.execute("INSERT INTO users VALUES (?, ?)", (1, "O'Brien"))
    cursor.execute("SELECT * FROM users")
    row = cursor.fetchone()
    assert row[0] == 1
    assert row[1] == "O'Brien"

    cursor.close()
    db.close()


def test_parameterized_error_count_mismatch():
    """Test error when parameter count doesn't match placeholders"""
    db = vibesql.connect()
    cursor = db.cursor()
    cursor.execute("CREATE TABLE users (id INTEGER, name VARCHAR(50))")

    # Too few parameters
    try:
        cursor.execute("INSERT INTO users VALUES (?, ?)", (1,))
        assert False, "Should have raised ProgrammingError"
    except vibesql.ProgrammingError as e:
        assert "Parameter count mismatch" in str(e)

    # Too many parameters
    try:
        cursor.execute("INSERT INTO users VALUES (?, ?)", (1, 'Alice', 'Extra'))
        assert False, "Should have raised ProgrammingError"
    except vibesql.ProgrammingError as e:
        assert "Parameter count mismatch" in str(e)

    cursor.close()
    db.close()


def test_parameterized_error_invalid_type():
    """Test error when parameter has invalid type"""
    db = vibesql.connect()
    cursor = db.cursor()
    cursor.execute("CREATE TABLE users (id INTEGER, name VARCHAR(50))")

    # Try to insert invalid type (dict)
    try:
        cursor.execute("INSERT INTO users VALUES (?, ?)", (1, {'invalid': 'dict'}))
        assert False, "Should have raised ProgrammingError"
    except vibesql.ProgrammingError as e:
        assert "invalid type" in str(e).lower() or "cannot convert" in str(e).lower()

    cursor.close()
    db.close()


def test_backward_compatibility_no_params():
    """Test backward compatibility - queries without parameters still work"""
    db = vibesql.connect()
    cursor = db.cursor()
    cursor.execute("CREATE TABLE users (id INTEGER, name VARCHAR(50))")
    cursor.execute("INSERT INTO users VALUES (1, 'Alice')")
    cursor.execute("SELECT * FROM users")
    rows = cursor.fetchall()
    assert len(rows) == 1
    assert rows[0] == (1, 'Alice')

    cursor.close()
    db.close()


def test_parameterized_complex_where():
    """Test parameterized query with multiple conditions"""
    db = vibesql.connect()
    cursor = db.cursor()
    cursor.execute("CREATE TABLE users (id INTEGER, age INTEGER, active BOOLEAN)")
    cursor.execute("INSERT INTO users VALUES (1, 25, TRUE)")
    cursor.execute("INSERT INTO users VALUES (2, 30, FALSE)")
    cursor.execute("INSERT INTO users VALUES (3, 35, TRUE)")
    cursor.execute("INSERT INTO users VALUES (4, 40, TRUE)")

    # Query with multiple parameters
    cursor.execute("SELECT * FROM users WHERE age > ? AND active = ?", (28, True))
    rows = cursor.fetchall()
    assert len(rows) == 2
    assert rows[0][0] == 3  # id=3, age=35
    assert rows[1][0] == 4  # id=4, age=40

    cursor.close()
    db.close()


def test_statement_cache_hit():
    """Test that repeated queries hit the statement cache"""
    db = vibesql.connect()
    cursor = db.cursor()
    cursor.execute("CREATE TABLE test (id INTEGER, value INTEGER)")

    # First execution - cache miss
    cursor.execute("INSERT INTO test VALUES (?, ?)", (1, 100))
    hits1, misses1, _ = cursor.cache_stats()

    # Second execution with same SQL pattern - cache hit
    cursor.execute("INSERT INTO test VALUES (?, ?)", (2, 200))
    hits2, misses2, _ = cursor.cache_stats()

    # Verify cache hit occurred
    assert hits2 == hits1 + 1, f"Expected cache hit, got hits={hits2}, misses={misses2}"
    assert misses2 == misses1, f"Expected no additional miss, got misses={misses2}"

    cursor.close()
    db.close()


def test_statement_cache_stats():
    """Test cache statistics tracking"""
    db = vibesql.connect()
    cursor = db.cursor()
    cursor.execute("CREATE TABLE test (id INTEGER, value INTEGER)")

    # Execute same query 10 times
    for i in range(10):
        cursor.execute("INSERT INTO test VALUES (?, ?)", (i, i * 10))

    hits, misses, hit_rate = cursor.cache_stats()

    # First execution is a miss, next 9 are hits
    assert misses >= 1, "Expected at least 1 cache miss"
    assert hits >= 9, f"Expected at least 9 cache hits, got {hits}"
    assert 0 <= hit_rate <= 1, f"Hit rate should be between 0 and 1, got {hit_rate}"

    cursor.close()
    db.close()


def test_statement_cache_invalidation():
    """Test that cache is invalidated on schema changes"""
    db = vibesql.connect()
    cursor = db.cursor()
    cursor.execute("CREATE TABLE test (id INTEGER)")

    # Execute some queries to populate cache
    cursor.execute("INSERT INTO test VALUES (?)", (1,))
    cursor.execute("INSERT INTO test VALUES (?)", (2,))
    hits1, misses1, _ = cursor.cache_stats()

    # Drop table should clear cache
    cursor.execute("DROP TABLE test")

    # Create new table and execute query
    cursor.execute("CREATE TABLE test (id INTEGER)")
    cursor.execute("INSERT INTO test VALUES (?)", (3,))

    # Cache should have been cleared, so we get a new miss
    hits2, misses2, _ = cursor.cache_stats()
    assert misses2 > misses1, "Expected cache to be cleared after DROP TABLE"

    cursor.close()
    db.close()


def test_statement_cache_hit_reuses_new_params_not_first_call():
    """Regression test for #6359.

    A cache HIT on repeated identical parameterized SQL text must bind THIS
    call's params, not silently replay the AST baked with the FIRST call's
    literal values. Prior to the fix, the second INSERT below wrote ('a',
    '1') again instead of ('b', '2'), because the cached AST already had the
    first call's literals substituted in.
    """
    db = vibesql.connect()
    cursor = db.cursor()
    cursor.execute("CREATE TABLE u (k VARCHAR(50), v VARCHAR(50))")

    # First call: cache miss, parses and caches the (unbound) AST.
    cursor.execute("INSERT INTO u VALUES (?, ?)", ("a", "1"))
    # Second call: identical SQL text -> cache HIT. Must bind these params,
    # not replay the first call's ("a", "1").
    cursor.execute("INSERT INTO u VALUES (?, ?)", ("b", "2"))

    hits, misses, _ = cursor.cache_stats()
    assert hits >= 1, f"Expected the second call to hit the statement cache, got hits={hits}"

    cursor.execute("SELECT * FROM u ORDER BY k")
    rows = cursor.fetchall()
    assert rows == [("a", "1"), ("b", "2")], (
        f"Expected distinct rows for distinct params, got {rows} "
        "(second insert replayed the first call's params - see #6359)"
    )

    cursor.close()
    db.close()


def test_statement_cache_hit_reuses_new_params_for_select():
    """Regression test for #6359: cache HIT on a repeated parameterized
    SELECT must use the new call's params, not the first call's."""
    db = vibesql.connect()
    cursor = db.cursor()
    cursor.execute("CREATE TABLE users (id INTEGER, name VARCHAR(50))")
    cursor.execute("INSERT INTO users VALUES (1, 'Alice')")
    cursor.execute("INSERT INTO users VALUES (2, 'Bob')")

    cursor.execute("SELECT name FROM users WHERE id = ?", (1,))
    first = cursor.fetchall()
    assert first == [("Alice",)]

    # Same SQL text, different param -> cache HIT, must bind id=2 this time.
    cursor.execute("SELECT name FROM users WHERE id = ?", (2,))
    second = cursor.fetchall()
    assert second == [("Bob",)], (
        f"Expected id=2 to resolve to Bob, got {second} (cache HIT replayed "
        "the first call's params - see #6359)"
    )

    cursor.close()
    db.close()


def test_parameterized_limit_offset():
    """`LIMIT ?` / `OFFSET ?` are ordinary DB-API pagination usage.

    Regression test for PR #6411: binding at AST level must cover LIMIT and
    OFFSET, not just WHERE. Before the binder/counter were made symmetric,
    the placeholder in LIMIT was invisible to the parameter counter, so this
    raised a spurious "Parameter count mismatch: expected 0, got 1".

    Each query is run TWICE with different parameter values so the second run
    exercises the statement-cache HIT path (the path #6359 was about).
    """
    db = vibesql.connect()
    cursor = db.cursor()
    cursor.execute("CREATE TABLE page (a INTEGER)")
    for i in range(1, 6):
        cursor.execute("INSERT INTO page VALUES (?)", (i,))

    # LIMIT ? — first call is a cache MISS, second an identical-SQL cache HIT
    # with a different value.
    cursor.execute("SELECT a FROM page ORDER BY a LIMIT ?", (2,))
    assert cursor.fetchall() == [(1,), (2,)]

    cursor.execute("SELECT a FROM page ORDER BY a LIMIT ?", (4,))
    assert cursor.fetchall() == [(1,), (2,), (3,), (4,)], (
        "LIMIT ? on a cache HIT must use this call's parameter, not the first call's"
    )

    # LIMIT ? OFFSET ? — same two-call pattern.
    cursor.execute("SELECT a FROM page ORDER BY a LIMIT ? OFFSET ?", (2, 1))
    assert cursor.fetchall() == [(2,), (3,)]

    cursor.execute("SELECT a FROM page ORDER BY a LIMIT ? OFFSET ?", (3, 2))
    assert cursor.fetchall() == [(3,), (4,), (5,)]

    # Mixed: a WHERE placeholder and a LIMIT placeholder in one statement.
    cursor.execute("SELECT a FROM page WHERE a > ? ORDER BY a LIMIT ?", (1, 2))
    assert cursor.fetchall() == [(2,), (3,)]

    cursor.execute("SELECT a FROM page WHERE a > ? ORDER BY a LIMIT ?", (3, 1))
    assert cursor.fetchall() == [(4,)]

    cursor.close()
    db.close()


def test_statement_cache_performance():
    """Test that cache provides performance improvement for repeated queries"""
    import time

    db = vibesql.connect()
    cursor = db.cursor()
    cursor.execute("CREATE TABLE test (id INTEGER, value INTEGER)")

    # Warm up and populate some data
    for i in range(100):
        cursor.execute("INSERT INTO test VALUES (?, ?)", (i, i * 10))

    # Execute 1000 UPDATEs using same SQL pattern (should hit cache)
    start_time = time.time()
    for i in range(1000):
        cursor.execute("UPDATE test SET value = value + 1 WHERE id = ?", (i % 100,))
    elapsed = time.time() - start_time

    # Check cache hit rate is high
    hits, misses, hit_rate = cursor.cache_stats()
    assert hit_rate > 0.95, f"Expected >95% cache hit rate, got {hit_rate:.2%}"

    # Performance should be reasonable (this is a soft assertion)
    # With cache, 1000 updates should complete quickly
    print(f"  1000 UPDATEs completed in {elapsed*1000:.2f}ms (cache hit rate: {hit_rate:.2%})")

    cursor.close()
    db.close()


# ============================================================================
# Schema Cache Tests (Issue #826)
# ============================================================================

def test_schema_cache_basic():
    """Test that schema cache reduces redundant catalog lookups"""
    db = vibesql.connect()
    cursor = db.cursor()
    cursor.execute("CREATE TABLE test (id INTEGER, value INTEGER)")
    cursor.execute("INSERT INTO test VALUES (1, 100)")

    # First UPDATE - schema cache miss
    cursor.execute("UPDATE test SET value = 200 WHERE id = 1")
    hits1, misses1, _ = cursor.schema_cache_stats()
    assert misses1 == 1, f"Expected 1 schema cache miss, got {misses1}"

    # Second UPDATE on same table - schema cache hit
    cursor.execute("UPDATE test SET value = 300 WHERE id = 1")
    hits2, misses2, _ = cursor.schema_cache_stats()
    assert hits2 == 1, f"Expected 1 schema cache hit, got {hits2}"
    assert misses2 == 1, f"Expected still 1 schema cache miss, got {misses2}"

    cursor.close()
    db.close()


def test_schema_cache_multiple_tables():
    """Test schema caching with multiple tables"""
    db = vibesql.connect()
    cursor = db.cursor()
    cursor.execute("CREATE TABLE table1 (id INTEGER, value INTEGER)")
    cursor.execute("CREATE TABLE table2 (id INTEGER, value INTEGER)")

    # UPDATE table1 - first miss for table1
    cursor.execute("UPDATE table1 SET value = 100 WHERE id = 1")
    hits1, misses1, _ = cursor.schema_cache_stats()

    # UPDATE table2 - first miss for table2
    cursor.execute("UPDATE table2 SET value = 200 WHERE id = 1")
    hits2, misses2, _ = cursor.schema_cache_stats()
    assert misses2 == misses1 + 1, "Expected miss for second table"

    # UPDATE table1 again - should hit cache
    cursor.execute("UPDATE table1 SET value = 101 WHERE id = 1")
    hits3, misses3, _ = cursor.schema_cache_stats()
    assert hits3 > hits2, "Expected schema cache hit for table1"

    cursor.close()
    db.close()


def test_schema_cache_invalidation_on_ddl():
    """Test that schema cache is cleared on DDL operations"""
    db = vibesql.connect()
    cursor = db.cursor()
    cursor.execute("CREATE TABLE test (id INTEGER, value INTEGER)")
    cursor.execute("INSERT INTO test VALUES (1, 100)")

    # Execute UPDATE to populate schema cache
    cursor.execute("UPDATE test SET value = 200 WHERE id = 1")
    hits1, misses1, _ = cursor.schema_cache_stats()
    assert misses1 == 1, "Expected initial schema cache miss"

    # DROP TABLE should clear schema cache
    cursor.execute("DROP TABLE test")

    # Recreate table and UPDATE - should be a new miss (cache was cleared)
    cursor.execute("CREATE TABLE test (id INTEGER, value INTEGER)")
    cursor.execute("INSERT INTO test VALUES (1, 100)")
    cursor.execute("UPDATE test SET value = 300 WHERE id = 1")

    hits2, misses2, _ = cursor.schema_cache_stats()
    assert misses2 > misses1, "Expected schema cache to be cleared after DDL"

    cursor.close()
    db.close()


def test_schema_cache_hit_rate():
    """Test schema cache hit rate with repeated UPDATEs"""
    db = vibesql.connect()
    cursor = db.cursor()
    cursor.execute("CREATE TABLE test (id INTEGER, value INTEGER)")

    # Insert test data
    for i in range(100):
        cursor.execute("INSERT INTO test VALUES (?, ?)", (i, i * 10))

    # Execute many UPDATEs on same table - should mostly hit schema cache
    for i in range(100):
        cursor.execute("UPDATE test SET value = value + 1 WHERE id = ?", (i,))

    hits, misses, hit_rate = cursor.schema_cache_stats()

    # First UPDATE is a miss, rest should be hits
    assert misses == 1, f"Expected exactly 1 schema cache miss, got {misses}"
    assert hits == 99, f"Expected 99 schema cache hits, got {hits}"
    assert abs(hit_rate - 0.99) < 0.01, f"Expected 99% hit rate, got {hit_rate:.2%}"

    print(f"  Schema cache: {hits} hits, {misses} misses, {hit_rate:.2%} hit rate")

    cursor.close()
    db.close()


def test_schema_cache_performance_improvement():
    """Test that schema caching reduces lookup overhead"""
    import time

    db = vibesql.connect()
    cursor = db.cursor()
    cursor.execute("CREATE TABLE test (id INTEGER, value INTEGER)")

    # Insert test data
    for i in range(100):
        cursor.execute("INSERT INTO test VALUES (?, ?)", (i, i * 10))

    # Execute many UPDATEs - schema should be cached after first lookup
    start_time = time.time()
    for i in range(1000):
        cursor.execute("UPDATE test SET value = value + 1 WHERE id = ?", (i % 100,))
    elapsed = time.time() - start_time

    # Verify high schema cache hit rate
    hits, misses, hit_rate = cursor.schema_cache_stats()
    assert hit_rate > 0.98, f"Expected >98% schema cache hit rate, got {hit_rate:.2%}"
    assert misses == 1, f"Expected exactly 1 schema cache miss, got {misses}"

    print(f"  1000 UPDATEs with schema cache: {elapsed*1000:.2f}ms (schema hit rate: {hit_rate:.2%})")

    cursor.close()
    db.close()


if __name__ == "__main__":
    # Run all tests
    test_connection()
    test_cursor_creation()
    test_create_table()
    test_insert_select()
    test_multiple_inserts()
    test_data_types()
    test_fetchone()
    test_fetchmany()
    test_update()
    test_delete()
    test_rowcount()
    test_drop_table()
    test_multiple_cursors()

    # Parameterized query tests
    test_parameterized_select()
    test_parameterized_insert()
    test_parameterized_update()
    test_parameterized_delete()
    test_parameterized_multiple_types()
    test_parameterized_null_value()
    test_parameterized_string_with_quotes()
    test_parameterized_error_count_mismatch()
    test_parameterized_error_invalid_type()
    test_backward_compatibility_no_params()
    test_parameterized_complex_where()

    # Statement cache tests
    test_statement_cache_hit()
    test_statement_cache_stats()
    test_statement_cache_invalidation()
    test_statement_cache_performance()

    # Schema cache tests
    test_schema_cache_basic()
    test_schema_cache_multiple_tables()
    test_schema_cache_invalidation_on_ddl()
    test_schema_cache_hit_rate()
    test_schema_cache_performance_improvement()

    print("All tests passed!")
