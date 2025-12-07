//! Test for Issue #3807: User-defined indexes not rebuilt after table compaction
//!
//! This regression test ensures that user-defined B-tree indexes are correctly
//! rebuilt when table compaction occurs (triggered when >50% of rows are deleted).
//!
//! Root cause: The `Table::compact()` function rebuilt only internal hash indexes
//! (for PK/UNIQUE constraints) but did not notify the Database layer to rebuild
//! user-defined B-tree indexes. After compaction, row indices change but the B-tree
//! indexes still contained stale indices, causing index scans to return incorrect results.
//!
//! Fix: `Table::delete_by_indices()` now returns `DeleteResult` with a `compacted` flag.
//! When `compacted` is true, callers must call `Database::rebuild_indexes()` to rebuild
//! user-defined indexes.

use vibesql_ast::IndexType;
use vibesql_executor::{CreateTableExecutor, DeleteExecutor, InsertExecutor, SelectExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

/// Helper to execute SQL and return results as string for comparison
fn execute(db: &mut Database, sql: &str) -> Result<Vec<Vec<SqlValue>>, String> {
    let stmt = Parser::parse_sql(sql).map_err(|e| format!("Parse error: {}", e))?;
    match stmt {
        vibesql_ast::Statement::Select(select_stmt) => {
            let executor = SelectExecutor::new(db);
            let rows = executor.execute(&select_stmt).map_err(|e| format!("Execution error: {}", e))?;
            Ok(rows.into_iter().map(|r| r.values.into_vec()).collect())
        }
        vibesql_ast::Statement::CreateTable(create_stmt) => {
            CreateTableExecutor::execute(&create_stmt, db)
                .map_err(|e| format!("Create table error: {}", e))?;
            Ok(vec![])
        }
        vibesql_ast::Statement::CreateIndex(create_idx_stmt) => {
            // Extract unique from IndexType
            let unique = matches!(create_idx_stmt.index_type, IndexType::BTree { unique: true });
            db.create_index(
                create_idx_stmt.index_name,
                create_idx_stmt.table_name,
                unique,
                create_idx_stmt.columns,
            ).map_err(|e| format!("Create index error: {}", e))?;
            Ok(vec![])
        }
        vibesql_ast::Statement::Insert(insert_stmt) => {
            InsertExecutor::execute(db, &insert_stmt)
                .map_err(|e| format!("Insert error: {}", e))?;
            Ok(vec![])
        }
        vibesql_ast::Statement::Delete(delete_stmt) => {
            DeleteExecutor::execute(&delete_stmt, db)
                .map_err(|e| format!("Delete error: {}", e))?;
            Ok(vec![])
        }
        _ => Err("Unexpected statement type".to_string()),
    }
}

/// Exact reproduction case from issue #3807
///
/// This tests the scenario where:
/// 1. Table tab0 is created and populated with 10 rows
/// 2. Table tab1 is created with a user-defined B-tree index
/// 3. Data is copied from tab0 to tab1 via INSERT...SELECT
/// 4. 70% of rows are deleted from tab1, triggering compaction
/// 5. Index scan should return the same results as table scan
#[test]
fn test_issue_3807_index_scan_after_compaction() {
    let mut db = Database::new();

    // Create and populate tab0 (source table)
    execute(&mut db, "CREATE TABLE tab0(pk INTEGER PRIMARY KEY, col0 INTEGER)").unwrap();
    execute(&mut db, "INSERT INTO tab0 VALUES(0, 14)").unwrap();
    execute(&mut db, "INSERT INTO tab0 VALUES(1, 81)").unwrap();
    execute(&mut db, "INSERT INTO tab0 VALUES(2, 96)").unwrap();
    execute(&mut db, "INSERT INTO tab0 VALUES(3, 58)").unwrap();
    execute(&mut db, "INSERT INTO tab0 VALUES(4, 97)").unwrap();
    execute(&mut db, "INSERT INTO tab0 VALUES(5, 34)").unwrap();
    execute(&mut db, "INSERT INTO tab0 VALUES(6, 47)").unwrap();
    execute(&mut db, "INSERT INTO tab0 VALUES(7, 29)").unwrap();
    execute(&mut db, "INSERT INTO tab0 VALUES(8, 54)").unwrap();
    execute(&mut db, "INSERT INTO tab0 VALUES(9, 38)").unwrap();

    // Create tab1 with a user-defined index
    execute(&mut db, "CREATE TABLE tab1(pk INTEGER PRIMARY KEY, col0 INTEGER)").unwrap();
    execute(&mut db, "CREATE INDEX idx_tab1_0 ON tab1 (col0)").unwrap();

    // Copy data from tab0 to tab1
    execute(&mut db, "INSERT INTO tab1 SELECT * FROM tab0").unwrap();

    // Verify data was copied correctly (10 rows)
    let before_delete = execute(&mut db, "SELECT pk, col0 FROM tab1 ORDER BY pk").unwrap();
    assert_eq!(before_delete.len(), 10, "Should have 10 rows before delete");

    // Delete 70% of rows (triggers compaction - threshold is >50%)
    execute(&mut db, "DELETE FROM tab1 WHERE pk IN (0, 1, 2, 5, 6, 7, 8)").unwrap();

    // Table scan should return 3 rows (pk 3, 4, 9)
    let table_scan_result = execute(&mut db, "SELECT pk, col0 FROM tab1 ORDER BY pk").unwrap();
    assert_eq!(table_scan_result.len(), 3, "Table scan should return 3 rows");

    // Verify the expected rows are present
    let expected_pks: Vec<i64> = vec![3, 4, 9];
    let actual_pks: Vec<i64> = table_scan_result
        .iter()
        .map(|row| match &row[0] {
            SqlValue::Integer(i) => *i,
            _ => panic!("Expected integer"),
        })
        .collect();
    assert_eq!(actual_pks, expected_pks, "Table scan returned wrong rows");

    // Index scan (col0 > 0) should return the SAME 3 rows
    // Before the fix, this would return 0 rows due to stale index entries
    let index_scan_result = execute(&mut db, "SELECT pk, col0 FROM tab1 WHERE col0 > 0 ORDER BY pk").unwrap();
    assert_eq!(
        index_scan_result.len(),
        3,
        "Index scan should return 3 rows (same as table scan). \
         If this fails with 0 rows, the bug is still present - index not rebuilt after compaction."
    );

    // Verify index scan returns same rows as table scan
    assert_eq!(
        table_scan_result, index_scan_result,
        "Index scan should return same results as table scan"
    );
}

/// Additional test: Single-row PK delete path (delete_by_pk_fast)
///
/// Tests that the fast PK delete path also correctly rebuilds indexes after compaction
#[test]
fn test_issue_3807_pk_delete_path_after_compaction() {
    let mut db = Database::new();

    // Create table with index
    execute(&mut db, "CREATE TABLE test_pk(id INTEGER PRIMARY KEY, val INTEGER)").unwrap();
    execute(&mut db, "CREATE INDEX idx_val ON test_pk (val)").unwrap();

    // Insert 10 rows
    for i in 0..10 {
        execute(&mut db, &format!("INSERT INTO test_pk VALUES({}, {})", i, i * 10)).unwrap();
    }

    // Delete rows one at a time using PK equality (triggers delete_by_pk_fast path)
    // Delete 7 rows (70%) to trigger compaction
    for pk in [0, 1, 2, 5, 6, 7, 8] {
        execute(&mut db, &format!("DELETE FROM test_pk WHERE id = {}", pk)).unwrap();
    }

    // Table scan
    let table_scan = execute(&mut db, "SELECT id, val FROM test_pk ORDER BY id").unwrap();
    assert_eq!(table_scan.len(), 3, "Should have 3 rows remaining");

    // Index scan (should match table scan)
    let index_scan = execute(&mut db, "SELECT id, val FROM test_pk WHERE val >= 0 ORDER BY id").unwrap();
    assert_eq!(
        index_scan.len(), 3,
        "Index scan should return 3 rows after compaction from PK delete path"
    );
    assert_eq!(table_scan, index_scan, "Index scan should match table scan");
}

/// Test: REPLACE statement path
///
/// Tests that REPLACE statement correctly handles index rebuild after compaction
#[test]
fn test_issue_3807_replace_path_after_compaction() {
    let mut db = Database::new();

    // Create table with index
    execute(&mut db, "CREATE TABLE test_replace(id INTEGER PRIMARY KEY, val INTEGER)").unwrap();
    execute(&mut db, "CREATE INDEX idx_replace_val ON test_replace (val)").unwrap();

    // Insert 10 rows
    for i in 0..10 {
        execute(&mut db, &format!("INSERT INTO test_replace VALUES({}, {})", i, i * 10)).unwrap();
    }

    // Use REPLACE to update existing rows (this deletes and reinserts)
    // Replace 7 rows - this triggers delete of existing + insert of new
    for pk in [0, 1, 2, 5, 6, 7, 8] {
        execute(&mut db, &format!("REPLACE INTO test_replace VALUES({}, {})", pk, pk * 100)).unwrap();
    }

    // Now delete those replaced rows to trigger compaction
    execute(&mut db, "DELETE FROM test_replace WHERE id IN (0, 1, 2, 5, 6, 7, 8)").unwrap();

    // Verify index scan works correctly after this complex operation
    let table_scan = execute(&mut db, "SELECT id, val FROM test_replace ORDER BY id").unwrap();
    let index_scan = execute(&mut db, "SELECT id, val FROM test_replace WHERE val >= 0 ORDER BY id").unwrap();

    assert_eq!(table_scan.len(), 3, "Should have 3 rows");
    assert_eq!(table_scan, index_scan, "Index scan should match table scan after REPLACE + compaction");
}
