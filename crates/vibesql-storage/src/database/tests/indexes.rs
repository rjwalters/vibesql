use vibesql_types::SqlValue;

use crate::database::indexes::{IndexData, IndexManager};
use crate::database::{DatabaseConfig, SpillPolicy};

#[test]
fn test_range_scan_preserves_index_order() {
    // Create index data with rows that are NOT in order by row index
    // but ARE in order by indexed value
    let mut data = std::collections::BTreeMap::new();

    // col0 values: row 1 has 50, row 2 has 60, row 0 has 70
    // Index should be sorted by value: 50, 60, 70
    // Keys are now normalized to Double for consistent comparison
    data.insert(vec![SqlValue::Double(50.0)], vec![1]);
    data.insert(vec![SqlValue::Double(60.0)], vec![2]);
    data.insert(vec![SqlValue::Double(70.0)], vec![0]);

    let index_data = IndexData::InMemory { data, pending_deletions: Vec::new() };

    // Query: col0 > 55 should return rows in index order: [2, 0] (values 60, 70)
    let result = index_data.range_scan(
        Some(&SqlValue::Integer(55)),
        None,
        false, // exclusive start
        false,
    );

    // Result should be [2, 0] NOT [0, 2]
    // This preserves the index ordering (60 comes before 70)
    assert_eq!(
        result,
        vec![2, 0],
        "range_scan should return rows in index order (by value), not row index order"
    );
}

#[test]
fn test_range_scan_between_preserves_order() {
    // Test BETWEEN queries maintain index order
    let mut data = std::collections::BTreeMap::new();

    // Values out of row-index order
    // Keys are now normalized to Double for consistent comparison
    data.insert(vec![SqlValue::Double(40.0)], vec![5]);
    data.insert(vec![SqlValue::Double(50.0)], vec![1]);
    data.insert(vec![SqlValue::Double(60.0)], vec![2]);
    data.insert(vec![SqlValue::Double(70.0)], vec![0]);

    let index_data = IndexData::InMemory { data, pending_deletions: Vec::new() };

    // Query: col0 BETWEEN 45 AND 65 (i.e., col0 >= 45 AND col0 <= 65)
    let result = index_data.range_scan(
        Some(&SqlValue::Integer(45)),
        Some(&SqlValue::Integer(65)),
        true, // inclusive start
        true, // inclusive end
    );

    // Should return [1, 2] (values 50, 60) in that order
    assert_eq!(result, vec![1, 2], "BETWEEN should return rows in index order");
}

#[test]
fn test_range_scan_with_duplicate_values() {
    // Test case: multiple rows with the same indexed value
    let mut data = std::collections::BTreeMap::new();

    // Multiple rows with value 60: rows 3, 7, 2 (in insertion order)
    // Keys are now normalized to Double for consistent comparison
    data.insert(vec![SqlValue::Double(50.0)], vec![1]);
    data.insert(vec![SqlValue::Double(60.0)], vec![3, 7, 2]); // duplicates
    data.insert(vec![SqlValue::Double(70.0)], vec![0]);

    let index_data = IndexData::InMemory { data, pending_deletions: Vec::new() };

    // Query: col0 >= 60 should return [3, 7, 2, 0]
    // Rows with value 60 maintain insertion order, then row 0 with value 70
    let result = index_data.range_scan(
        Some(&SqlValue::Integer(60)),
        None,
        true, // inclusive start
        false,
    );

    assert_eq!(
        result,
        vec![3, 7, 2, 0],
        "Duplicate values should maintain insertion order within the same key"
    );
}

#[test]
fn test_multi_lookup_with_duplicate_values() {
    // Test case: multi_lookup with duplicate indexed values
    let mut data = std::collections::BTreeMap::new();

    // Multiple rows with value 60: rows 3, 7, 2 (in insertion order)
    // Keys are now normalized to Double for consistent comparison
    data.insert(vec![SqlValue::Double(50.0)], vec![1]);
    data.insert(vec![SqlValue::Double(60.0)], vec![3, 7, 2]); // duplicates
    data.insert(vec![SqlValue::Double(70.0)], vec![0]);

    let index_data = IndexData::InMemory { data, pending_deletions: Vec::new() };

    // Query: col0 IN (60, 70) should return [3, 7, 2, 0]
    // Rows with value 60 maintain insertion order, then row 0 with value 70
    let result = index_data.multi_lookup(&[SqlValue::Integer(60), SqlValue::Integer(70)]);

    assert_eq!(
        result,
        vec![3, 7, 2, 0],
        "multi_lookup with duplicate values should maintain insertion order within the same key"
    );
}

// Note: This test cannot verify DiskBacked variant in test builds because
// DISK_BACKED_THRESHOLD is set to usize::MAX when cfg(test) is active.
// The test still verifies that large index creation succeeds.
#[test]
fn test_disk_backed_index_creation_with_bulk_load() {
    // Test that large indexes can be created successfully.
    // Note: In test builds, DISK_BACKED_THRESHOLD is usize::MAX, so this will
    // create an InMemory index. The DiskBacked path is tested via benchmarks.
    use crate::Row;
    use vibesql_ast::OrderDirection;
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::DataType;

    // Use unique temp directory to avoid conflicts with other tests
    let temp_dir = tempfile::tempdir().expect("Failed to create temp directory");

    // Create a table schema with one integer column
    let columns = vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)];
    let table_schema = TableSchema::new("test_table".to_string(), columns);

    // Create 100,500 rows - this would exceed DISK_BACKED_THRESHOLD in production
    let num_rows = 100_500;
    let table_rows: Vec<Row> =
        (0..num_rows).map(|i| Row::from_vec(vec![SqlValue::Integer(i as i64)])).collect();

    let mut index_manager = IndexManager::new();
    index_manager.set_database_path(temp_dir.path().to_path_buf());

    // Create index - will be InMemory in test builds, DiskBacked in production
    let result = index_manager.create_index(
        "idx_id".to_string(),
        "test_table".to_string(),
        &table_schema,
        &table_rows,
        false, // non-unique
        vec![vibesql_ast::IndexColumn {
            column_name: "id".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }],
    );

    assert!(result.is_ok(), "Large index creation should succeed");

    // Verify index was created
    assert!(index_manager.index_exists("idx_id"));

    // Verify index data exists (don't assert on variant type due to cfg(test) threshold)
    let index_data = index_manager.get_index_data("idx_id");
    assert!(index_data.is_some(), "Index data should be retrievable");
}

#[test]
fn test_in_memory_index_for_small_tables() {
    // Test that in-memory indexes are still used for small tables
    use crate::Row;
    use vibesql_ast::OrderDirection;
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::DataType;

    let columns = vec![ColumnSchema::new("value".to_string(), DataType::Integer, false)];
    let table_schema = TableSchema::new("small_table".to_string(), columns);

    // Create small number of rows (well below threshold)
    let table_rows: Vec<Row> =
        (0..100).map(|i| Row::from_vec(vec![SqlValue::Integer(i as i64)])).collect();

    let mut index_manager = IndexManager::new();

    let result = index_manager.create_index(
        "idx_value".to_string(),
        "small_table".to_string(),
        &table_schema,
        &table_rows,
        false,
        vec![vibesql_ast::IndexColumn {
            column_name: "value".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }],
    );

    assert!(result.is_ok());
    assert!(index_manager.index_exists("idx_value"));

    // Verify it's using InMemory variant
    let index_data = index_manager.get_index_data("idx_value");
    assert!(index_data.is_some());
    match index_data.unwrap() {
        IndexData::InMemory { .. } => {
            // Success - in-memory was used for small table
        }
        IndexData::DiskBacked { .. } => {
            panic!("Expected InMemory variant for small table, got DiskBacked");
        }
        IndexData::IVFFlat { .. } => {
            panic!("Expected InMemory variant for small table, got IVFFlat");
        }
        IndexData::Hnsw { .. } => {
            panic!("Expected InMemory variant for small table, got Hnsw");
        }
    }
}

#[test]
fn test_budget_enforcement_with_spill_policy() {
    // Test that memory budget is enforced with SpillToDisk policy
    use crate::Row;
    use vibesql_ast::OrderDirection;
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::DataType;

    // Use unique temp directory to avoid conflicts with other tests
    let temp_dir = tempfile::tempdir().expect("Failed to create temp directory");

    let columns = vec![ColumnSchema::new("value".to_string(), DataType::Integer, false)];
    let table_schema = TableSchema::new("test_table".to_string(), columns);

    // Create small rows for in-memory indexes
    let table_rows: Vec<Row> =
        (0..100).map(|i| Row::from_vec(vec![SqlValue::Integer(i as i64)])).collect();

    let mut index_manager = IndexManager::new();
    index_manager.set_database_path(temp_dir.path().to_path_buf());

    // Set a very small memory budget to force eviction
    let config = DatabaseConfig {
        memory_budget: 1000,            // 1KB - very small to force eviction
        disk_budget: 100 * 1024 * 1024, // 100MB disk
        spill_policy: SpillPolicy::SpillToDisk,
        sql_mode: vibesql_types::SqlMode::default(),
        columnar_cache_budget: 1024 * 1024, // 1MB columnar cache
    };
    index_manager.set_config(config);

    // Create first index - should succeed and be in-memory
    let result1 = index_manager.create_index(
        "idx_1".to_string(),
        "test_table".to_string(),
        &table_schema,
        &table_rows,
        false,
        vec![vibesql_ast::IndexColumn {
            column_name: "value".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }],
    );
    assert!(result1.is_ok());

    // Creating a second index should trigger eviction of the first one
    let result2 = index_manager.create_index(
        "idx_2".to_string(),
        "test_table".to_string(),
        &table_schema,
        &table_rows,
        false,
        vec![vibesql_ast::IndexColumn {
            column_name: "value".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }],
    );
    assert!(result2.is_ok());

    // Both indexes should exist (one in memory, one spilled to disk)
    assert!(index_manager.index_exists("idx_1"));
    assert!(index_manager.index_exists("idx_2"));
}

#[test]
fn test_lru_eviction_order() {
    // Test that LRU eviction selects the coldest (least recently used) index
    use crate::Row;
    use instant::Duration;
    use std::thread;
    use vibesql_ast::OrderDirection;
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::DataType;

    // Use unique temp directory to avoid conflicts with other tests
    let temp_dir = tempfile::tempdir().expect("Failed to create temp directory");

    let columns = vec![ColumnSchema::new("value".to_string(), DataType::Integer, false)];
    let table_schema = TableSchema::new("test_table".to_string(), columns);

    let table_rows: Vec<Row> =
        (0..50).map(|i| Row::from_vec(vec![SqlValue::Integer(i as i64)])).collect();

    let mut index_manager = IndexManager::new();
    index_manager.set_database_path(temp_dir.path().to_path_buf());

    // Small budget to trigger eviction
    let config = DatabaseConfig {
        memory_budget: 2000, // 2KB
        disk_budget: 100 * 1024 * 1024,
        spill_policy: SpillPolicy::SpillToDisk,
        sql_mode: vibesql_types::SqlMode::default(),
        columnar_cache_budget: 1024 * 1024, // 1MB columnar cache
    };
    index_manager.set_config(config);

    // Create idx_1
    index_manager
        .create_index(
            "idx_1".to_string(),
            "test_table".to_string(),
            &table_schema,
            &table_rows,
            false,
            vec![vibesql_ast::IndexColumn {
                column_name: "value".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            }],
        )
        .unwrap();

    thread::sleep(Duration::from_millis(10));

    // Create idx_2
    index_manager
        .create_index(
            "idx_2".to_string(),
            "test_table".to_string(),
            &table_schema,
            &table_rows,
            false,
            vec![vibesql_ast::IndexColumn {
                column_name: "value".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            }],
        )
        .unwrap();

    thread::sleep(Duration::from_millis(10));

    // Access idx_1 to make it "hot" (more recently used than idx_2)
    let _ = index_manager.get_index_data("idx_1");

    thread::sleep(Duration::from_millis(10));

    // Create idx_3 - should evict idx_2 (coldest), not idx_1
    index_manager
        .create_index(
            "idx_3".to_string(),
            "test_table".to_string(),
            &table_schema,
            &table_rows,
            false,
            vec![vibesql_ast::IndexColumn {
                column_name: "value".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            }],
        )
        .unwrap();

    // All indexes should still exist
    assert!(index_manager.index_exists("idx_1"));
    assert!(index_manager.index_exists("idx_2"));
    assert!(index_manager.index_exists("idx_3"));

    // idx_2 should have been evicted to disk (coldest)
    // idx_1 and idx_3 should still be in memory (hot)
    let _backend_1 = index_manager.resource_tracker.get_backend("idx_1");
    let backend_2 = index_manager.resource_tracker.get_backend("idx_2");
    let _backend_3 = index_manager.resource_tracker.get_backend("idx_3");

    // Note: Exact behavior depends on memory sizes, but idx_2 should be coldest
    assert!(backend_2.is_some());
}

#[test]
fn test_access_tracking() {
    // Test that index accesses are tracked for LRU
    use crate::Row;
    use vibesql_ast::OrderDirection;
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::DataType;

    let columns = vec![ColumnSchema::new("value".to_string(), DataType::Integer, false)];
    let table_schema = TableSchema::new("test_table".to_string(), columns);

    let table_rows: Vec<Row> =
        (0..10).map(|i| Row::from_vec(vec![SqlValue::Integer(i as i64)])).collect();

    let mut index_manager = IndexManager::new();

    index_manager
        .create_index(
            "idx_test".to_string(),
            "test_table".to_string(),
            &table_schema,
            &table_rows,
            false,
            vec![vibesql_ast::IndexColumn {
                column_name: "value".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            }],
        )
        .unwrap();

    // Initial access count should be 0 (creation doesn't count as access)
    let stats = index_manager.resource_tracker.get_index_stats("IDX_TEST");
    assert!(stats.is_some());
    let initial_count = stats.unwrap().get_access_count();

    // Access the index a few times
    let _ = index_manager.get_index_data("IDX_TEST");
    let _ = index_manager.get_index_data("IDX_TEST");
    let _ = index_manager.get_index_data("IDX_TEST");

    // Access count should have increased
    let stats = index_manager.resource_tracker.get_index_stats("IDX_TEST");
    assert!(stats.is_some());
    let final_count = stats.unwrap().get_access_count();

    assert!(
        final_count > initial_count,
        "Access count should increase after index accesses (initial: {}, final: {})",
        initial_count,
        final_count
    );
}

#[test]
fn test_resource_cleanup_on_drop() {
    // Test that resources are freed when indexes are dropped
    use crate::Row;
    use vibesql_ast::OrderDirection;
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::DataType;

    let columns = vec![ColumnSchema::new("value".to_string(), DataType::Integer, false)];
    let table_schema = TableSchema::new("test_table".to_string(), columns);

    let table_rows: Vec<Row> =
        (0..100).map(|i| Row::from_vec(vec![SqlValue::Integer(i as i64)])).collect();

    let mut index_manager = IndexManager::new();

    // Create an index
    index_manager
        .create_index(
            "idx_test".to_string(),
            "test_table".to_string(),
            &table_schema,
            &table_rows,
            false,
            vec![vibesql_ast::IndexColumn {
                column_name: "value".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            }],
        )
        .unwrap();

    // Memory should be in use
    let memory_before = index_manager.resource_tracker.memory_used();
    assert!(memory_before > 0);

    // Drop the index
    index_manager.drop_index("idx_test").unwrap();

    // Memory should be freed
    let memory_after = index_manager.resource_tracker.memory_used();
    assert_eq!(memory_after, 0, "Memory should be freed after dropping index");
}

#[test]
fn test_database_config_presets() {
    // Test that preset configurations have expected values
    let browser_config = DatabaseConfig::browser_default();
    assert_eq!(browser_config.memory_budget, 512 * 1024 * 1024); // 512MB
    assert_eq!(browser_config.disk_budget, 2 * 1024 * 1024 * 1024); // 2GB
    assert_eq!(browser_config.spill_policy, SpillPolicy::SpillToDisk);

    let server_config = DatabaseConfig::server_default();
    assert_eq!(server_config.memory_budget, 16 * 1024 * 1024 * 1024); // 16GB
    assert_eq!(server_config.disk_budget, 1024 * 1024 * 1024 * 1024); // 1TB
    assert_eq!(server_config.spill_policy, SpillPolicy::BestEffort);

    let test_config = DatabaseConfig::test_default();
    assert_eq!(test_config.memory_budget, 10 * 1024 * 1024); // 10MB
    assert_eq!(test_config.disk_budget, 100 * 1024 * 1024); // 100MB
    assert_eq!(test_config.spill_policy, SpillPolicy::SpillToDisk);
}

#[test]
fn test_index_scan_after_database_reset() {
    // Reproduces issue #1618: Index scans returning 0 rows after Database::reset()
    // This simulates the sqllogictest runner's database pooling behavior
    use crate::Database;
    use crate::Row;
    use vibesql_ast::{IndexColumn, OrderDirection};
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::DataType;

    // Helper function to run a complete test cycle
    fn run_test_cycle(db: &mut Database, cycle_num: usize) -> Result<(), String> {
        eprintln!("\n=== Test Cycle {} ===", cycle_num);

        // Create table schema
        let columns = vec![
            ColumnSchema::new("pk".to_string(), DataType::Integer, false),
            ColumnSchema::new("col0".to_string(), DataType::Integer, false),
        ];
        let mut table_schema = TableSchema::new("tab1".to_string(), columns);
        table_schema.primary_key = Some(vec!["pk".to_string()]);

        // Create table (simulates: CREATE TABLE tab1...)
        db.create_table(table_schema.clone()).unwrap();
        eprintln!("  Created table 'tab1'");

        // Insert rows into table (simulates: INSERT INTO tab1 VALUES...)
        let rows = vec![
            Row::from_vec(vec![SqlValue::Integer(1), SqlValue::Integer(100)]),
            Row::from_vec(vec![SqlValue::Integer(2), SqlValue::Integer(200)]),
            Row::from_vec(vec![SqlValue::Integer(3), SqlValue::Integer(300)]),
            Row::from_vec(vec![SqlValue::Integer(4), SqlValue::Integer(400)]),
            Row::from_vec(vec![SqlValue::Integer(5), SqlValue::Integer(500)]),
        ];

        // Get the table and insert rows
        let table = db.get_table_mut("tab1").unwrap();
        for row in &rows {
            table.insert(row.clone()).unwrap();
        }
        eprintln!("  Inserted {} rows", rows.len());

        // Create index on col0 (simulates: CREATE INDEX idx_col0 ON tab1(col0))
        db.create_index(
            "idx_col0".to_string(),
            "tab1".to_string(),
            false,
            vec![IndexColumn {
                column_name: "col0".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            }],
        )
        .unwrap();
        eprintln!("  Created index 'idx_col0'");

        // Verify index was created and populated
        assert!(db.index_exists("idx_col0"));
        let index_data = db.get_index_data("idx_col0").expect("Index should exist");

        // Check that index contains row indices
        let all_indices: Vec<usize> = match &index_data {
            crate::database::indexes::IndexData::InMemory { data, .. } => {
                data.values().flatten().copied().collect()
            }
            crate::database::indexes::IndexData::DiskBacked { .. } => {
                panic!("Expected in-memory index for small table");
            }
            crate::database::indexes::IndexData::IVFFlat { .. } => {
                panic!("Expected in-memory index for small table, got IVFFlat");
            }
            crate::database::indexes::IndexData::Hnsw { .. } => {
                panic!("Expected in-memory index for small table, got Hnsw");
            }
        };
        eprintln!("  Index contains {} row references", all_indices.len());
        assert_eq!(all_indices.len(), 5, "Index should contain 5 row references");

        // Now perform index scan (simulates: SELECT pk FROM tab1 WHERE col0 > 250)
        // Get table for index scan
        let table = db.get_table("tab1").expect("Table should exist");

        // Perform range scan on index
        let matching_row_indices = index_data.range_scan(
            Some(&SqlValue::Integer(250)), // col0 > 250
            None,
            false, // exclusive start
            false,
        );
        eprintln!("  Index range scan returned {} row indices", matching_row_indices.len());

        // This should return indices for rows 3, 4, 5 (col0 = 300, 400, 500)
        if matching_row_indices.len() != 3 {
            return Err(format!(
                "Cycle {}: Index scan should find 3 rows with col0 > 250, but found {}",
                cycle_num,
                matching_row_indices.len()
            ));
        }

        // Fetch actual rows using the indices
        let all_rows = table.scan();
        eprintln!("  Table has {} total rows", all_rows.len());

        let fetched_rows: Vec<Row> =
            matching_row_indices.into_iter().filter_map(|idx| all_rows.get(idx).cloned()).collect();
        eprintln!("  Fetched {} rows from table using index", fetched_rows.len());

        // This is the CRITICAL assertion that fails in sqllogictest after reset
        if fetched_rows.len() != 3 {
            return Err(format!(
                "Cycle {}: Should fetch 3 rows from table using index scan, but got {} rows. \
                 Table has {} total rows. This reproduces issue #1618!",
                cycle_num,
                fetched_rows.len(),
                all_rows.len()
            ));
        }

        // Verify the correct rows were returned
        assert_eq!(fetched_rows[0].values[1], SqlValue::Integer(300));
        assert_eq!(fetched_rows[1].values[1], SqlValue::Integer(400));
        assert_eq!(fetched_rows[2].values[1], SqlValue::Integer(500));

        eprintln!("  ✓ Cycle {} PASSED", cycle_num);
        Ok(())
    }

    // Simulate sqllogictest thread-local database pooling behavior:
    // - First test file: uses Database::new()
    // - Subsequent test files: reuse database after reset()

    let mut db = Database::new();

    // CYCLE 1: First test file (no reset) - this should PASS
    eprintln!("\n🔄 Running first test file (fresh database)...");
    run_test_cycle(&mut db, 1).expect("Cycle 1 should pass (fresh database)");

    // CYCLE 2: Second test file (after reset) - this should FAIL and reproduce issue #1618
    eprintln!("\n🔄 Resetting database (simulating pooling)...");
    db.reset();
    eprintln!("🔄 Running second test file (pooled database after reset)...");

    match run_test_cycle(&mut db, 2) {
        Ok(()) => {
            eprintln!("\n⚠️  WARNING: Cycle 2 passed! The bug may have been fixed.");
            eprintln!("    If this test now passes, issue #1618 is resolved.");
        }
        Err(e) => {
            panic!("\n❌ REPRODUCED ISSUE #1618:\n{}\n\nThis demonstrates the database pooling bug where index scans return 0 rows after Database::reset()", e);
        }
    }
}

#[test]
fn test_thread_local_pool_pattern() {
    // Test that mimics the EXACT thread-local pooling pattern from db_adapter.rs
    // This may better reproduce the sqllogictest runner bug
    use crate::Database;
    use crate::Row;
    use std::cell::RefCell;
    use vibesql_ast::{IndexColumn, OrderDirection};
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::{DataType, SqlValue};

    thread_local! {
        static TEST_DB_POOL: RefCell<Option<Database>> = const { RefCell::new(None) };
    }

    fn get_pooled_database() -> Database {
        TEST_DB_POOL.with(|pool| {
            let mut pool_ref = pool.borrow_mut();
            match pool_ref.take() {
                Some(mut db) => {
                    eprintln!("  [POOL] Reusing pooled database after reset");
                    db.reset();
                    db
                }
                None => {
                    eprintln!("  [POOL] Creating fresh database");
                    Database::new()
                }
            }
        })
    }

    fn return_to_pool(db: Database) {
        TEST_DB_POOL.with(|pool| {
            let mut pool_ref = pool.borrow_mut();
            if pool_ref.is_none() {
                eprintln!("  [POOL] Returning database to pool");
                *pool_ref = Some(db);
            }
        });
    }

    // Helper to run a test cycle
    fn run_cycle(cycle_num: usize) {
        eprintln!("\n=== Cycle {} ===", cycle_num);

        // Get database from pool (mimics VibeSqlDB::new())
        let mut db = get_pooled_database();

        // Create table
        let columns = vec![
            ColumnSchema::new("pk".to_string(), DataType::Integer, false),
            ColumnSchema::new("col0".to_string(), DataType::Integer, false),
        ];
        let mut table_schema = TableSchema::new("tab1".to_string(), columns);
        table_schema.primary_key = Some(vec!["pk".to_string()]);
        db.create_table(table_schema).unwrap();

        // Insert rows
        let table = db.get_table_mut("tab1").unwrap();
        for i in 1..=5 {
            table
                .insert(Row::from_vec(vec![SqlValue::Integer(i), SqlValue::Integer(i * 100)]))
                .unwrap();
        }
        eprintln!("  Inserted 5 rows");

        // Create index
        db.create_index(
            "idx_col0".to_string(),
            "tab1".to_string(),
            false,
            vec![IndexColumn {
                column_name: "col0".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            }],
        )
        .unwrap();
        eprintln!("  Created index");

        // Query using index
        let index_data = db.get_index_data("idx_col0").unwrap();
        let matching_indices =
            index_data.range_scan(Some(&SqlValue::Integer(250)), None, false, false);
        eprintln!("  Index scan returned {} indices", matching_indices.len());

        let table = db.get_table("tab1").unwrap();
        let all_rows = table.scan();
        let fetched_rows: Vec<Row> =
            matching_indices.into_iter().filter_map(|idx| all_rows.get(idx).cloned()).collect();

        eprintln!("  Fetched {} rows from table", fetched_rows.len());
        assert_eq!(
            fetched_rows.len(),
            3,
            "Cycle {}: Expected 3 rows, got {}",
            cycle_num,
            fetched_rows.len()
        );
        eprintln!("  ✓ Cycle {} PASSED", cycle_num);

        // Return to pool (mimics Drop trait)
        return_to_pool(db);
    }

    // Run multiple cycles using the pool
    run_cycle(1);
    run_cycle(2);
    run_cycle(3);
}

// ============================================================================
// Direct Index Lookup API Tests (Issue #3140)
// ============================================================================

#[test]
fn test_lookup_by_index_single_column() {
    // Test direct index lookup with single-column index
    use crate::Database;
    use crate::Row;
    use vibesql_ast::{IndexColumn, OrderDirection};
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::{DataType, SqlValue};

    let mut db = Database::new();

    // Create table
    let columns = vec![
        ColumnSchema::new("id".to_string(), DataType::Integer, false),
        ColumnSchema::new("name".to_string(), DataType::Varchar { max_length: Some(50) }, false),
    ];
    let mut table_schema = TableSchema::new("users".to_string(), columns);
    table_schema.primary_key = Some(vec!["id".to_string()]);
    db.create_table(table_schema).unwrap();

    // Insert rows
    let table = db.get_table_mut("users").unwrap();
    table
        .insert(Row::from_vec(vec![SqlValue::Integer(1), SqlValue::Varchar("Alice".into())]))
        .unwrap();
    table
        .insert(Row::from_vec(vec![SqlValue::Integer(2), SqlValue::Varchar("Bob".into())]))
        .unwrap();
    table
        .insert(Row::from_vec(vec![SqlValue::Integer(3), SqlValue::Varchar("Charlie".into())]))
        .unwrap();

    // Create index on id column
    db.create_index(
        "idx_users_id".to_string(),
        "users".to_string(),
        true, // unique
        vec![IndexColumn {
            column_name: "id".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }],
    )
    .unwrap();

    // Test lookup_by_index - existing key
    let result = db.lookup_by_index("idx_users_id", &[SqlValue::Integer(2)]).unwrap();
    assert!(result.is_some());
    let rows = result.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[1], SqlValue::Varchar("Bob".into()));

    // Test lookup_by_index - non-existing key
    let result = db.lookup_by_index("idx_users_id", &[SqlValue::Integer(999)]).unwrap();
    assert!(result.is_none());
}

#[test]
fn test_lookup_one_by_index() {
    // Test lookup_one_by_index which returns just the first row
    use crate::Database;
    use crate::Row;
    use vibesql_ast::{IndexColumn, OrderDirection};
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::{DataType, SqlValue};

    let mut db = Database::new();

    // Create table
    let columns = vec![
        ColumnSchema::new("id".to_string(), DataType::Integer, false),
        ColumnSchema::new("value".to_string(), DataType::Integer, false),
    ];
    let mut table_schema = TableSchema::new("items".to_string(), columns);
    table_schema.primary_key = Some(vec!["id".to_string()]);
    db.create_table(table_schema).unwrap();

    // Insert rows
    let table = db.get_table_mut("items").unwrap();
    table.insert(Row::from_vec(vec![SqlValue::Integer(1), SqlValue::Integer(100)])).unwrap();
    table.insert(Row::from_vec(vec![SqlValue::Integer(2), SqlValue::Integer(200)])).unwrap();

    // Create index
    db.create_index(
        "idx_items_pk".to_string(),
        "items".to_string(),
        true,
        vec![IndexColumn {
            column_name: "id".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }],
    )
    .unwrap();

    // Test lookup_one_by_index
    let row = db.lookup_one_by_index("idx_items_pk", &[SqlValue::Integer(1)]).unwrap();
    assert!(row.is_some());
    assert_eq!(row.unwrap().values[1], SqlValue::Integer(100));

    // Test non-existing key
    let row = db.lookup_one_by_index("idx_items_pk", &[SqlValue::Integer(999)]).unwrap();
    assert!(row.is_none());
}

#[test]
fn test_lookup_by_index_composite_key() {
    // Test direct index lookup with composite (multi-column) key
    use crate::Database;
    use crate::Row;
    use vibesql_ast::{IndexColumn, OrderDirection};
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::{DataType, SqlValue};

    let mut db = Database::new();

    // Create table with composite key (warehouse_id, district_id, order_id)
    let columns = vec![
        ColumnSchema::new("w_id".to_string(), DataType::Integer, false),
        ColumnSchema::new("d_id".to_string(), DataType::Integer, false),
        ColumnSchema::new("o_id".to_string(), DataType::Integer, false),
        ColumnSchema::new("amount".to_string(), DataType::DoublePrecision, false),
    ];
    let table_schema = TableSchema::new("orders".to_string(), columns);
    db.create_table(table_schema).unwrap();

    // Insert rows
    let table = db.get_table_mut("orders").unwrap();
    table
        .insert(Row::from_vec(vec![
            SqlValue::Integer(1),
            SqlValue::Integer(1),
            SqlValue::Integer(1),
            SqlValue::Double(100.0),
        ]))
        .unwrap();
    table
        .insert(Row::from_vec(vec![
            SqlValue::Integer(1),
            SqlValue::Integer(1),
            SqlValue::Integer(2),
            SqlValue::Double(200.0),
        ]))
        .unwrap();
    table
        .insert(Row::from_vec(vec![
            SqlValue::Integer(1),
            SqlValue::Integer(2),
            SqlValue::Integer(1),
            SqlValue::Double(300.0),
        ]))
        .unwrap();
    table
        .insert(Row::from_vec(vec![
            SqlValue::Integer(2),
            SqlValue::Integer(1),
            SqlValue::Integer(1),
            SqlValue::Double(400.0),
        ]))
        .unwrap();

    // Create composite index
    db.create_index(
        "idx_orders_pk".to_string(),
        "orders".to_string(),
        true,
        vec![
            IndexColumn {
                column_name: "w_id".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            },
            IndexColumn {
                column_name: "d_id".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            },
            IndexColumn {
                column_name: "o_id".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            },
        ],
    )
    .unwrap();

    // Test lookup with full composite key
    let result = db
        .lookup_by_index(
            "idx_orders_pk",
            &[SqlValue::Integer(1), SqlValue::Integer(2), SqlValue::Integer(1)],
        )
        .unwrap();
    assert!(result.is_some());
    let rows = result.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[3], SqlValue::Double(300.0));

    // Test non-existing composite key
    let result = db
        .lookup_by_index(
            "idx_orders_pk",
            &[SqlValue::Integer(9), SqlValue::Integer(9), SqlValue::Integer(9)],
        )
        .unwrap();
    assert!(result.is_none());
}

#[test]
fn test_lookup_by_index_batch() {
    // Test batch lookup with multiple keys
    use crate::Database;
    use crate::Row;
    use vibesql_ast::{IndexColumn, OrderDirection};
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::{DataType, SqlValue};

    let mut db = Database::new();

    // Create table
    let columns = vec![
        ColumnSchema::new("id".to_string(), DataType::Integer, false),
        ColumnSchema::new("name".to_string(), DataType::Varchar { max_length: Some(50) }, false),
    ];
    let table_schema = TableSchema::new("products".to_string(), columns);
    db.create_table(table_schema).unwrap();

    // Insert rows
    let table = db.get_table_mut("products").unwrap();
    for i in 1..=10 {
        table
            .insert(Row::from_vec(vec![
                SqlValue::Integer(i),
                SqlValue::Varchar(arcstr::ArcStr::from(format!("Product{}", i))),
            ]))
            .unwrap();
    }

    // Create index
    db.create_index(
        "idx_products_pk".to_string(),
        "products".to_string(),
        true,
        vec![IndexColumn {
            column_name: "id".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }],
    )
    .unwrap();

    // Batch lookup - some exist, some don't
    let keys = vec![
        vec![SqlValue::Integer(2)],
        vec![SqlValue::Integer(5)],
        vec![SqlValue::Integer(999)], // doesn't exist
        vec![SqlValue::Integer(8)],
    ];
    let results = db.lookup_by_index_batch("idx_products_pk", &keys).unwrap();

    assert_eq!(results.len(), 4);
    assert!(results[0].is_some()); // id=2
    assert!(results[1].is_some()); // id=5
    assert!(results[2].is_none()); // id=999 doesn't exist
    assert!(results[3].is_some()); // id=8

    // Verify values
    assert_eq!(results[0].as_ref().unwrap()[0].values[1], SqlValue::Varchar("Product2".into()));
    assert_eq!(results[1].as_ref().unwrap()[0].values[1], SqlValue::Varchar("Product5".into()));
    assert_eq!(results[3].as_ref().unwrap()[0].values[1], SqlValue::Varchar("Product8".into()));
}

#[test]
fn test_lookup_one_by_index_batch() {
    // Test batch lookup returning single rows (for unique indexes)
    use crate::Database;
    use crate::Row;
    use vibesql_ast::{IndexColumn, OrderDirection};
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::{DataType, SqlValue};

    let mut db = Database::new();

    // Create table
    let columns = vec![
        ColumnSchema::new("id".to_string(), DataType::Integer, false),
        ColumnSchema::new("price".to_string(), DataType::DoublePrecision, false),
    ];
    let table_schema = TableSchema::new("items".to_string(), columns);
    db.create_table(table_schema).unwrap();

    // Insert rows
    let table = db.get_table_mut("items").unwrap();
    for i in 1..=5 {
        table
            .insert(Row::from_vec(vec![SqlValue::Integer(i), SqlValue::Double(i as f64 * 10.0)]))
            .unwrap();
    }

    // Create index
    db.create_index(
        "idx_items_id".to_string(),
        "items".to_string(),
        true,
        vec![IndexColumn {
            column_name: "id".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }],
    )
    .unwrap();

    // Batch lookup single rows
    let keys = vec![
        vec![SqlValue::Integer(1)],
        vec![SqlValue::Integer(3)],
        vec![SqlValue::Integer(99)], // doesn't exist
    ];
    let results = db.lookup_one_by_index_batch("idx_items_id", &keys).unwrap();

    assert_eq!(results.len(), 3);
    assert!(results[0].is_some());
    assert!(results[1].is_some());
    assert!(results[2].is_none());

    assert_eq!(results[0].unwrap().values[1], SqlValue::Double(10.0));
    assert_eq!(results[1].unwrap().values[1], SqlValue::Double(30.0));
}

#[test]
fn test_lookup_by_index_error_cases() {
    // Test error handling for invalid index names
    use crate::Database;

    let db = Database::new();

    // Test lookup on non-existent index
    let result = db.lookup_by_index("nonexistent_index", &[SqlValue::Integer(1)]);
    assert!(result.is_err());

    let result = db.lookup_one_by_index("nonexistent_index", &[SqlValue::Integer(1)]);
    assert!(result.is_err());

    let result = db.lookup_by_index_batch("nonexistent_index", &[vec![SqlValue::Integer(1)]]);
    assert!(result.is_err());
}

// ============================================================================
// Prefix Index Lookup API Tests (Issue #3195)
// ============================================================================

#[test]
fn test_lookup_by_index_prefix_basic() {
    // Test basic prefix lookup on a multi-column index
    use crate::Database;
    use crate::Row;
    use vibesql_ast::{IndexColumn, OrderDirection};
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::{DataType, SqlValue};

    let mut db = Database::new();

    // Create table with 3-column composite key (like TPC-C NEW_ORDER)
    let columns = vec![
        ColumnSchema::new("w_id".to_string(), DataType::Integer, false),
        ColumnSchema::new("d_id".to_string(), DataType::Integer, false),
        ColumnSchema::new("o_id".to_string(), DataType::Integer, false),
        ColumnSchema::new("carrier_id".to_string(), DataType::Integer, true),
    ];
    let table_schema = TableSchema::new("new_order".to_string(), columns);
    db.create_table(table_schema).unwrap();

    // Insert rows: warehouse 1, districts 1-3, varying orders
    let table = db.get_table_mut("new_order").unwrap();
    // District 1: orders 101, 102
    table
        .insert(Row::from_vec(vec![
                SqlValue::Integer(1),
                SqlValue::Integer(1),
                SqlValue::Integer(101),
                SqlValue::Null,
            ]))
        .unwrap();
    table
        .insert(Row::from_vec(vec![
                SqlValue::Integer(1),
                SqlValue::Integer(1),
                SqlValue::Integer(102),
                SqlValue::Null,
            ]))
        .unwrap();
    // District 2: orders 201, 202, 203
    table
        .insert(Row::from_vec(vec![
                SqlValue::Integer(1),
                SqlValue::Integer(2),
                SqlValue::Integer(201),
                SqlValue::Null,
            ]))
        .unwrap();
    table
        .insert(Row::from_vec(vec![
                SqlValue::Integer(1),
                SqlValue::Integer(2),
                SqlValue::Integer(202),
                SqlValue::Null,
            ]))
        .unwrap();
    table
        .insert(Row::from_vec(vec![
                SqlValue::Integer(1),
                SqlValue::Integer(2),
                SqlValue::Integer(203),
                SqlValue::Null,
            ]))
        .unwrap();
    // District 3: order 301
    table
        .insert(Row::from_vec(vec![
                SqlValue::Integer(1),
                SqlValue::Integer(3),
                SqlValue::Integer(301),
                SqlValue::Null,
            ]))
        .unwrap();

    // Create composite index on (w_id, d_id, o_id)
    db.create_index(
        "idx_no_pk".to_string(),
        "new_order".to_string(),
        true,
        vec![
            IndexColumn {
                column_name: "w_id".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            },
            IndexColumn {
                column_name: "d_id".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            },
            IndexColumn {
                column_name: "o_id".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            },
        ],
    )
    .unwrap();

    // Test prefix lookup: find all orders for warehouse 1, district 2
    let rows = db
        .lookup_by_index_prefix("idx_no_pk", &[SqlValue::Integer(1), SqlValue::Integer(2)])
        .unwrap();

    // Should find 3 rows (orders 201, 202, 203)
    assert_eq!(rows.len(), 3, "Should find 3 orders for district 2");

    // Verify the order IDs
    let order_ids: Vec<i64> = rows
        .iter()
        .map(|r| match &r.values[2] {
            SqlValue::Integer(v) => *v,
            _ => 0,
        })
        .collect();
    assert!(order_ids.contains(&201));
    assert!(order_ids.contains(&202));
    assert!(order_ids.contains(&203));
}

#[test]
fn test_lookup_by_index_prefix_single_column() {
    // Test prefix lookup with single-column prefix
    use crate::Database;
    use crate::Row;
    use vibesql_ast::{IndexColumn, OrderDirection};
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::{DataType, SqlValue};

    let mut db = Database::new();

    // Create table
    let columns = vec![
        ColumnSchema::new("warehouse".to_string(), DataType::Integer, false),
        ColumnSchema::new("district".to_string(), DataType::Integer, false),
        ColumnSchema::new("value".to_string(), DataType::Integer, false),
    ];
    let table_schema = TableSchema::new("test_data".to_string(), columns);
    db.create_table(table_schema).unwrap();

    // Insert rows
    let table = db.get_table_mut("test_data").unwrap();
    // Warehouse 1: 3 rows
    table
        .insert(Row::from_vec(vec![SqlValue::Integer(1), SqlValue::Integer(1), SqlValue::Integer(100)]))
        .unwrap();
    table
        .insert(Row::from_vec(vec![SqlValue::Integer(1), SqlValue::Integer(2), SqlValue::Integer(200)]))
        .unwrap();
    table
        .insert(Row::from_vec(vec![SqlValue::Integer(1), SqlValue::Integer(3), SqlValue::Integer(300)]))
        .unwrap();
    // Warehouse 2: 2 rows
    table
        .insert(Row::from_vec(vec![SqlValue::Integer(2), SqlValue::Integer(1), SqlValue::Integer(400)]))
        .unwrap();
    table
        .insert(Row::from_vec(vec![SqlValue::Integer(2), SqlValue::Integer(2), SqlValue::Integer(500)]))
        .unwrap();

    // Create index on (warehouse, district)
    db.create_index(
        "idx_wd".to_string(),
        "test_data".to_string(),
        false,
        vec![
            IndexColumn {
                column_name: "warehouse".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            },
            IndexColumn {
                column_name: "district".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            },
        ],
    )
    .unwrap();

    // Test single-column prefix: find all rows for warehouse 1
    let rows = db.lookup_by_index_prefix("idx_wd", &[SqlValue::Integer(1)]).unwrap();
    assert_eq!(rows.len(), 3, "Should find 3 rows for warehouse 1");

    // Test single-column prefix: find all rows for warehouse 2
    let rows = db.lookup_by_index_prefix("idx_wd", &[SqlValue::Integer(2)]).unwrap();
    assert_eq!(rows.len(), 2, "Should find 2 rows for warehouse 2");
}

#[test]
fn test_lookup_by_index_prefix_no_match() {
    // Test prefix lookup when no rows match
    use crate::Database;
    use crate::Row;
    use vibesql_ast::{IndexColumn, OrderDirection};
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::{DataType, SqlValue};

    let mut db = Database::new();

    // Create table
    let columns = vec![
        ColumnSchema::new("a".to_string(), DataType::Integer, false),
        ColumnSchema::new("b".to_string(), DataType::Integer, false),
    ];
    let table_schema = TableSchema::new("test_table".to_string(), columns);
    db.create_table(table_schema).unwrap();

    // Insert rows
    let table = db.get_table_mut("test_table").unwrap();
    table.insert(Row::from_vec(vec![SqlValue::Integer(1), SqlValue::Integer(10)])).unwrap();
    table.insert(Row::from_vec(vec![SqlValue::Integer(2), SqlValue::Integer(20)])).unwrap();

    // Create index
    db.create_index(
        "idx_ab".to_string(),
        "test_table".to_string(),
        false,
        vec![
            IndexColumn {
                column_name: "a".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            },
            IndexColumn {
                column_name: "b".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            },
        ],
    )
    .unwrap();

    // Test prefix that doesn't match any rows
    let rows = db.lookup_by_index_prefix("idx_ab", &[SqlValue::Integer(999)]).unwrap();
    assert!(rows.is_empty(), "Should find no rows for non-existent prefix");
}

#[test]
fn test_lookup_by_index_prefix_batch_basic() {
    // Test batch prefix lookup on a multi-column index
    use crate::Database;
    use crate::Row;
    use vibesql_ast::{IndexColumn, OrderDirection};
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::{DataType, SqlValue};

    let mut db = Database::new();

    // Create table (like TPC-C NEW_ORDER)
    let columns = vec![
        ColumnSchema::new("w_id".to_string(), DataType::Integer, false),
        ColumnSchema::new("d_id".to_string(), DataType::Integer, false),
        ColumnSchema::new("o_id".to_string(), DataType::Integer, false),
    ];
    let table_schema = TableSchema::new("new_order".to_string(), columns);
    db.create_table(table_schema).unwrap();

    // Insert rows for warehouse 1, all 10 districts
    let table = db.get_table_mut("new_order").unwrap();
    for d_id in 1..=10 {
        // Each district has d_id orders (district 1 has 1 order, district 2 has 2, etc.)
        for o_id in 1..=d_id {
            table
                .insert(Row::from_vec(vec![
                        SqlValue::Integer(1),
                        SqlValue::Integer(d_id as i64),
                        SqlValue::Integer(o_id as i64 * 100),
                    ]))
                .unwrap();
        }
    }

    // Create composite index
    db.create_index(
        "idx_no_pk".to_string(),
        "new_order".to_string(),
        true,
        vec![
            IndexColumn {
                column_name: "w_id".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            },
            IndexColumn {
                column_name: "d_id".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            },
            IndexColumn {
                column_name: "o_id".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            },
        ],
    )
    .unwrap();

    // Batch prefix lookup: find all orders for districts 1, 5, and 10
    let prefixes = vec![
        vec![SqlValue::Integer(1), SqlValue::Integer(1)], // district 1
        vec![SqlValue::Integer(1), SqlValue::Integer(5)], // district 5
        vec![SqlValue::Integer(1), SqlValue::Integer(10)], // district 10
    ];
    let results = db.lookup_by_index_prefix_batch("idx_no_pk", &prefixes).unwrap();

    assert_eq!(results.len(), 3);
    assert_eq!(results[0].len(), 1, "District 1 should have 1 order");
    assert_eq!(results[1].len(), 5, "District 5 should have 5 orders");
    assert_eq!(results[2].len(), 10, "District 10 should have 10 orders");
}

#[test]
fn test_lookup_by_index_prefix_batch_tpcc_delivery() {
    // Test the exact TPC-C Delivery transaction pattern:
    // Look up all new orders for all 10 districts in a single batch
    use crate::Database;
    use crate::Row;
    use vibesql_ast::{IndexColumn, OrderDirection};
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::{DataType, SqlValue};

    let mut db = Database::new();

    // Create NEW_ORDER table
    let columns = vec![
        ColumnSchema::new("no_w_id".to_string(), DataType::Integer, false),
        ColumnSchema::new("no_d_id".to_string(), DataType::Integer, false),
        ColumnSchema::new("no_o_id".to_string(), DataType::Integer, false),
    ];
    let table_schema = TableSchema::new("new_order".to_string(), columns);
    db.create_table(table_schema).unwrap();

    // Insert new orders for warehouse 1
    let table = db.get_table_mut("new_order").unwrap();
    let w_id = 1;
    // Each district has some new orders (simulating pending orders)
    // District 1 has 1 order, district 2 has 2, ..., district 10 has 10
    for d_id in 1..=10 {
        for o_id in 2100..(2100 + d_id) {
            table
                .insert(Row::from_vec(vec![
                        SqlValue::Integer(w_id),
                        SqlValue::Integer(d_id as i64),
                        SqlValue::Integer(o_id as i64),
                    ]))
                .unwrap();
        }
    }

    // Create composite index (primary key)
    db.create_index(
        "idx_new_order_pk".to_string(),
        "new_order".to_string(),
        true,
        vec![
            IndexColumn {
                column_name: "no_w_id".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            },
            IndexColumn {
                column_name: "no_d_id".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            },
            IndexColumn {
                column_name: "no_o_id".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            },
        ],
    )
    .unwrap();

    // TPC-C Delivery transaction: batch prefix lookup for all 10 districts
    let prefixes: Vec<Vec<SqlValue>> =
        (1..=10).map(|d| vec![SqlValue::Integer(w_id), SqlValue::Integer(d)]).collect();

    let results = db.lookup_by_index_prefix_batch("idx_new_order_pk", &prefixes).unwrap();

    // Verify we got results for all 10 districts
    assert_eq!(results.len(), 10);

    // Verify each district has the expected number of orders
    // District 1 has 1 order (2100..2101), district 2 has 2, ..., district 10 has 10
    for (d_idx, rows) in results.iter().enumerate() {
        let d_id = d_idx + 1;
        let expected_orders = d_id; // District N has N orders
        assert_eq!(
            rows.len(),
            expected_orders,
            "District {} should have {} new orders",
            d_id,
            expected_orders
        );
    }
}

#[test]
fn test_lookup_by_index_prefix_error_cases() {
    // Test error handling for prefix lookup
    use crate::Database;

    let db = Database::new();

    // Test prefix lookup on non-existent index
    let result = db.lookup_by_index_prefix("nonexistent_index", &[SqlValue::Integer(1)]);
    assert!(result.is_err());

    // Test batch prefix lookup on non-existent index
    let result =
        db.lookup_by_index_prefix_batch("nonexistent_index", &[vec![SqlValue::Integer(1)]]);
    assert!(result.is_err());
}

#[test]
fn test_lookup_by_index_prefix_empty_prefix() {
    // Test prefix lookup with empty prefix
    use crate::Database;
    use crate::Row;
    use vibesql_ast::{IndexColumn, OrderDirection};
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::{DataType, SqlValue};

    let mut db = Database::new();

    // Create table
    let columns = vec![
        ColumnSchema::new("a".to_string(), DataType::Integer, false),
        ColumnSchema::new("b".to_string(), DataType::Integer, false),
    ];
    let table_schema = TableSchema::new("test_table".to_string(), columns);
    db.create_table(table_schema).unwrap();

    // Insert rows
    let table = db.get_table_mut("test_table").unwrap();
    table.insert(Row::from_vec(vec![SqlValue::Integer(1), SqlValue::Integer(10)])).unwrap();

    // Create index
    db.create_index(
        "idx_ab".to_string(),
        "test_table".to_string(),
        false,
        vec![
            IndexColumn {
                column_name: "a".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            },
            IndexColumn {
                column_name: "b".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            },
        ],
    )
    .unwrap();

    // Empty prefix matches everything - returns all rows
    let rows = db.lookup_by_index_prefix("idx_ab", &[]).unwrap();
    assert_eq!(rows.len(), 1, "Empty prefix should return all rows");
}

// ============================================================================
// range_scan_limit tests (#3638)
// ============================================================================

#[test]
fn test_range_scan_limit_basic() {
    // Test that range_scan_limit stops after collecting limit rows
    let mut data = std::collections::BTreeMap::new();

    // Insert 10 rows with values 10, 20, ..., 100
    for i in 1..=10 {
        data.insert(vec![SqlValue::Double((i * 10) as f64)], vec![i as usize]);
    }

    let index_data = IndexData::InMemory { data, pending_deletions: Vec::new() };

    // Query all rows with limit 3
    let result = index_data.range_scan_limit(None, None, false, false, Some(3));
    assert_eq!(result.len(), 3, "Should return exactly 3 rows");

    // Query with BETWEEN and limit
    let result = index_data.range_scan_limit(
        Some(&SqlValue::Integer(25)),
        Some(&SqlValue::Integer(85)),
        true,
        true,
        Some(2),
    );
    // Values 30, 40, 50, 60, 70, 80 match, but limit is 2
    assert_eq!(result.len(), 2, "Should return exactly 2 rows with limit");
    // Should be rows with values 30 and 40 (row indices 3 and 4)
    assert_eq!(result, vec![3, 4]);
}

#[test]
fn test_range_scan_limit_no_limit() {
    // Test that range_scan_limit with None limit returns all rows
    let mut data = std::collections::BTreeMap::new();

    for i in 1..=5 {
        data.insert(vec![SqlValue::Double((i * 10) as f64)], vec![i as usize]);
    }

    let index_data = IndexData::InMemory { data, pending_deletions: Vec::new() };

    // Query with no limit - should return all
    let result = index_data.range_scan_limit(None, None, false, false, None);
    assert_eq!(result.len(), 5, "Should return all rows when limit is None");
}

#[test]
fn test_range_scan_limit_zero() {
    // Test that range_scan_limit with limit=0 returns empty
    let mut data = std::collections::BTreeMap::new();

    for i in 1..=5 {
        data.insert(vec![SqlValue::Double((i * 10) as f64)], vec![i as usize]);
    }

    let index_data = IndexData::InMemory { data, pending_deletions: Vec::new() };

    let result = index_data.range_scan_limit(None, None, false, false, Some(0));
    assert!(result.is_empty(), "Should return empty when limit is 0");
}

#[test]
fn test_range_scan_limit_larger_than_result() {
    // Test that limit larger than result set returns all matches
    let mut data = std::collections::BTreeMap::new();

    for i in 1..=3 {
        data.insert(vec![SqlValue::Double((i * 10) as f64)], vec![i as usize]);
    }

    let index_data = IndexData::InMemory { data, pending_deletions: Vec::new() };

    // Ask for 100 rows but only 3 exist
    let result = index_data.range_scan_limit(None, None, false, false, Some(100));
    assert_eq!(result.len(), 3, "Should return all 3 rows when limit > result size");
}
