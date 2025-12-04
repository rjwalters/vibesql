#!/usr/bin/env python3
"""
Test PEP 249 (DB-API 2.0) compliance for VibeSQL Python bindings
"""

import vibesql
import sys

def test_module_attributes():
    """Test required module-level attributes"""
    print("Testing module-level attributes...")
    
    # Test apilevel
    assert hasattr(vibesql, 'apilevel'), "Missing apilevel attribute"
    assert vibesql.apilevel == "2.0", f"apilevel should be '2.0', got {vibesql.apilevel}"
    print("  ✓ apilevel = '2.0'")
    
    # Test threadsafety
    assert hasattr(vibesql, 'threadsafety'), "Missing threadsafety attribute"
    assert vibesql.threadsafety == 1, f"threadsafety should be 1, got {vibesql.threadsafety}"
    print("  ✓ threadsafety = 1")
    
    # Test paramstyle
    assert hasattr(vibesql, 'paramstyle'), "Missing paramstyle attribute"
    assert vibesql.paramstyle == "qmark", f"paramstyle should be 'qmark', got {vibesql.paramstyle}"
    print("  ✓ paramstyle = 'qmark'")

def test_exception_hierarchy():
    """Test exception hierarchy"""
    print("\nTesting exception hierarchy...")
    
    # Test base exceptions
    assert hasattr(vibesql, 'Warning'), "Missing Warning exception"
    assert hasattr(vibesql, 'Error'), "Missing Error exception"
    assert hasattr(vibesql, 'InterfaceError'), "Missing InterfaceError exception"
    assert hasattr(vibesql, 'DatabaseError'), "Missing DatabaseError exception"
    assert hasattr(vibesql, 'DataError'), "Missing DataError exception"
    assert hasattr(vibesql, 'OperationalError'), "Missing OperationalError exception"
    assert hasattr(vibesql, 'IntegrityError'), "Missing IntegrityError exception"
    assert hasattr(vibesql, 'InternalError'), "Missing InternalError exception"
    assert hasattr(vibesql, 'ProgrammingError'), "Missing ProgrammingError exception"
    assert hasattr(vibesql, 'NotSupportedError'), "Missing NotSupportedError exception"
    print("  ✓ All PEP 249 exception classes present")
    
    # Test hierarchy
    assert issubclass(vibesql.Error, Exception), "Error should be subclass of Exception"
    assert issubclass(vibesql.DatabaseError, vibesql.Error), "DatabaseError should be subclass of Error"
    assert issubclass(vibesql.OperationalError, vibesql.DatabaseError), "OperationalError should be subclass of DatabaseError"
    assert issubclass(vibesql.ProgrammingError, vibesql.DatabaseError), "ProgrammingError should be subclass of DatabaseError"
    print("  ✓ Exception hierarchy is correct")

def test_connection_commit():
    """Test Connection.commit() method"""
    print("\nTesting Connection.commit()...")
    
    db = vibesql.connect()
    
    # Create table
    cursor = db.cursor()
    cursor.execute("CREATE TABLE test_commit (id INTEGER, value TEXT)")
    cursor.execute("INSERT INTO test_commit VALUES (1, 'hello')")
    
    # Call commit (should not raise error)
    db.commit()
    print("  ✓ commit() method works")
    
    # Verify data is still there
    cursor.execute("SELECT * FROM test_commit")
    result = cursor.fetchall()
    assert len(result) == 1, "Data should persist after commit"
    assert result[0] == (1, 'hello'), f"Expected (1, 'hello'), got {result[0]}"
    print("  ✓ Data persists after commit")

def test_cursor_description():
    """Test Cursor.description attribute"""
    print("\nTesting Cursor.description...")
    
    db = vibesql.connect()
    cursor = db.cursor()
    
    # Before any query, description should be None
    assert cursor.description is None, "description should be None before any query"
    print("  ✓ description is None before query")
    
    # After SELECT, description should contain column info
    cursor.execute("SELECT 1 as id, 'test' as name")
    assert cursor.description is not None, "description should not be None after SELECT"
    assert len(cursor.description) == 2, f"Expected 2 columns, got {len(cursor.description)}"
    print("  ✓ description has correct number of columns")
    
    # Check structure of description
    for col_desc in cursor.description:
        assert len(col_desc) == 7, f"Column descriptor should have 7 items, got {len(col_desc)}"
        assert isinstance(col_desc[0], str), f"First item (name) should be string, got {type(col_desc[0])}"
    print("  ✓ description has correct structure (7-item tuples)")
    
    # Check column names (note: VibeSQL uppercases column names)
    assert cursor.description[0][0].upper() == 'ID', f"Expected first column 'ID', got {cursor.description[0][0]}"
    assert cursor.description[1][0].upper() == 'NAME', f"Expected second column 'NAME', got {cursor.description[1][0]}"
    print("  ✓ Column names are correct")
    
    # After DML/DDL, description should be None again
    cursor.execute("CREATE TABLE desc_test (x INTEGER)")
    assert cursor.description is None, "description should be None after CREATE"
    print("  ✓ description is None after DDL")

def test_cursor_executemany():
    """Test Cursor.executemany() method"""
    print("\nTesting Cursor.executemany()...")
    
    db = vibesql.connect()
    cursor = db.cursor()
    
    # Create table
    cursor.execute("CREATE TABLE executemany_test (id INTEGER, value TEXT)")
    
    # Use executemany to insert multiple rows
    rows = [
        (1, 'Alice'),
        (2, 'Bob'),
        (3, 'Charlie'),
    ]
    cursor.executemany("INSERT INTO executemany_test VALUES (?, ?)", rows)
    print("  ✓ executemany() executed successfully")
    
    # Check rowcount reflects total inserted
    assert cursor.rowcount == 3, f"Expected rowcount=3, got {cursor.rowcount}"
    print("  ✓ rowcount is correct after executemany")
    
    # Verify all rows were inserted
    cursor.execute("SELECT * FROM executemany_test")
    results = cursor.fetchall()
    assert len(results) == 3, f"Expected 3 rows, got {len(results)}"
    print("  ✓ All rows were inserted")
    
    # Verify data
    assert results[0] == (1, 'Alice'), f"First row mismatch: {results[0]}"
    assert results[1] == (2, 'Bob'), f"Second row mismatch: {results[1]}"
    assert results[2] == (3, 'Charlie'), f"Third row mismatch: {results[2]}"
    print("  ✓ All row data is correct")

def test_executemany_updates():
    """Test executemany with UPDATE statements"""
    print("\nTesting executemany with UPDATE...")
    
    db = vibesql.connect()
    cursor = db.cursor()
    
    # Create and populate table
    cursor.execute("CREATE TABLE update_test (id INTEGER, value TEXT)")
    cursor.executemany("INSERT INTO update_test VALUES (?, ?)", [(1, 'a'), (2, 'b'), (3, 'c')])
    
    # Use executemany to update multiple rows
    updates = [
        ('A', 1),
        ('B', 2),
        ('C', 3),
    ]
    cursor.executemany("UPDATE update_test SET value = ? WHERE id = ?", updates)
    print("  ✓ executemany() with UPDATE works")
    
    # Verify updates
    cursor.execute("SELECT * FROM update_test ORDER BY id")
    results = cursor.fetchall()
    assert results[0] == (1, 'A'), f"Update failed for id=1: {results[0]}"
    assert results[1] == (2, 'B'), f"Update failed for id=2: {results[1]}"
    assert results[2] == (3, 'C'), f"Update failed for id=3: {results[2]}"
    print("  ✓ All updates applied correctly")

def test_executemany_deletes():
    """Test executemany with DELETE statements"""
    print("\nTesting executemany with DELETE...")
    
    db = vibesql.connect()
    cursor = db.cursor()
    
    # Create and populate table
    cursor.execute("CREATE TABLE delete_test (id INTEGER, value TEXT)")
    cursor.executemany("INSERT INTO delete_test VALUES (?, ?)", [(1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')])
    
    # Use executemany to delete multiple rows
    deletes = [(1,), (3,)]
    cursor.executemany("DELETE FROM delete_test WHERE id = ?", deletes)
    print("  ✓ executemany() with DELETE works")
    
    # Verify deletes
    cursor.execute("SELECT * FROM delete_test ORDER BY id")
    results = cursor.fetchall()
    assert len(results) == 2, f"Expected 2 rows after delete, got {len(results)}"
    assert results[0][0] == 2, f"Row with id=2 should exist, got {results[0]}"
    assert results[1][0] == 4, f"Row with id=4 should exist, got {results[1]}"
    print("  ✓ All deletes applied correctly")

def run_all_tests():
    """Run all tests"""
    print("=" * 60)
    print("PEP 249 (DB-API 2.0) Compliance Tests")
    print("=" * 60)
    
    try:
        test_module_attributes()
        test_exception_hierarchy()
        test_connection_commit()
        test_cursor_description()
        test_cursor_executemany()
        test_executemany_updates()
        test_executemany_deletes()
        
        print("\n" + "=" * 60)
        print("✓ All tests passed!")
        print("=" * 60)
        return 0
    except AssertionError as e:
        print(f"\n✗ Test failed: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1

if __name__ == '__main__':
    sys.exit(run_all_tests())
