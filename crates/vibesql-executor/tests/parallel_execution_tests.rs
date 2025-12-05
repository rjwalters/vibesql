//! Tests for parallel WHERE clause execution with automatic hardware-aware heuristics

use vibesql_executor::SelectExecutor;
use vibesql_storage::Database;

/// Helper function to parse SELECT statements
fn parse_select(sql: &str) -> vibesql_ast::SelectStmt {
    match vibesql_parser::Parser::parse_sql(sql) {
        Ok(vibesql_ast::Statement::Select(select_stmt)) => *select_stmt,
        _ => panic!("Failed to parse SELECT statement: {}", sql),
    }
}

/// Helper to create test database with large table for parallel execution testing
fn setup_large_test_db(num_rows: usize) -> Database {
    let mut db = Database::new();

    let schema = vibesql_catalog::TableSchema::new(
        "TEST_PARALLEL".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "value".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert rows
    for i in 0..num_rows {
        let row = vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(i as i64),
            vibesql_types::SqlValue::Integer((i % 100) as i64),
        ]);
        db.insert_row("TEST_PARALLEL", row).unwrap();
    }

    db
}

#[test]
fn test_parallel_filter_produces_correct_results() {
    // Create database with a table containing enough rows to trigger automatic parallel execution
    // The exact threshold depends on hardware (2k-20k rows), so we use 15k to ensure it's above threshold
    let db = setup_large_test_db(15_000);
    let executor = SelectExecutor::new(&db);

    // Test 1: Simple WHERE clause filtering
    // With automatic parallelism enabled, this should use parallel execution on 4+ core systems
    let stmt = parse_select("SELECT id FROM TEST_PARALLEL WHERE value < 10");
    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1500); // 15000 / 100 * 10 = 1500 rows

    // Test 2: Complex WHERE clause with multiple conditions
    let stmt2 = parse_select("SELECT id FROM TEST_PARALLEL WHERE value >= 20 AND value < 30");
    let result2 = executor.execute(&stmt2).unwrap();
    assert_eq!(result2.len(), 1500); // 15000 / 100 * 10 = 1500 rows

    // Test 3: Verify we can still disable parallelism if needed
    std::env::set_var("PARALLEL_THRESHOLD", "max");
    let result3 = executor.execute(&stmt).unwrap();
    std::env::remove_var("PARALLEL_THRESHOLD");

    // Result should be the same whether parallel or sequential
    assert_eq!(result3.len(), 1500);
}

#[test]
fn test_parallel_filter_below_threshold_uses_sequential() {
    // Create database with fewer rows than any hardware threshold (all thresholds >= 2000)
    let db = setup_large_test_db(1_000);
    let executor = SelectExecutor::new(&db);

    // With automatic parallelism, this should use sequential execution
    // because row count is below all hardware thresholds
    let stmt = parse_select("SELECT id FROM TEST_PARALLEL WHERE value < 10");
    let result = executor.execute(&stmt).unwrap();

    // With 1000 rows and value = i % 100, we have 10 rows per value
    // value < 10 means values 0-9, so 10 * 10 = 100 rows
    assert_eq!(result.len(), 100);
}

#[test]
fn test_parallel_filter_with_null_values() {
    let mut db = Database::new();

    let schema = vibesql_catalog::TableSchema::new(
        "TEST_NULLS".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "value".to_string(),
                vibesql_types::DataType::Integer,
                true,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert rows with some NULL values (12k rows to trigger automatic parallelism)
    for i in 0..12_000 {
        let value = if i % 10 == 0 {
            vibesql_types::SqlValue::Null
        } else {
            vibesql_types::SqlValue::Integer((i % 100) as i64)
        };
        let row =
            vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(i as i64), value]);
        db.insert_row("TEST_NULLS", row).unwrap();
    }

    let executor = SelectExecutor::new(&db);

    // Test with automatic parallelism (should parallelize on 4+ core systems)
    let stmt = parse_select("SELECT id FROM TEST_NULLS WHERE value IS NOT NULL AND value < 10");
    let result_auto = executor.execute(&stmt).unwrap();

    // Test with forced sequential execution for comparison
    std::env::set_var("PARALLEL_THRESHOLD", "max");
    let result_seq = executor.execute(&stmt).unwrap();
    std::env::remove_var("PARALLEL_THRESHOLD");

    // Both should produce the same results
    assert_eq!(result_auto.len(), result_seq.len());
}

#[test]
fn test_parallel_automatic_on_modern_hardware() {
    let db = setup_large_test_db(11_000); // Above all thresholds
    let executor = SelectExecutor::new(&db);

    // With automatic parallelism, this should work correctly regardless of whether
    // it uses parallel or sequential execution (depends on hardware)
    let stmt = parse_select("SELECT id FROM TEST_PARALLEL WHERE id < 100");
    let result = executor.execute(&stmt).unwrap();

    assert_eq!(result.len(), 100);

    // Verify we can force sequential if needed
    std::env::set_var("PARALLEL_THRESHOLD", "max");
    let result_seq = executor.execute(&stmt).unwrap();
    std::env::remove_var("PARALLEL_THRESHOLD");

    assert_eq!(result_seq.len(), 100);
}

#[test]
fn test_parallel_execution_integration_with_executor() {
    // Integration test: Verify automatic parallelism works correctly through SelectExecutor
    let db = setup_large_test_db(15_000); // Above all thresholds
    let executor = SelectExecutor::new(&db);

    // Test 1: Query with automatic parallelism (parallel on 4+ cores, sequential on fewer)
    let stmt = parse_select("SELECT id FROM TEST_PARALLEL WHERE value < 10");
    let result_auto = executor.execute(&stmt).unwrap();
    assert_eq!(result_auto.len(), 1500);

    // Test 2: Force sequential execution using PARALLEL_THRESHOLD=max
    std::env::set_var("PARALLEL_THRESHOLD", "max");
    let result_seq = executor.execute(&stmt).unwrap();
    std::env::remove_var("PARALLEL_THRESHOLD");

    // Both should produce same count (order may differ)
    assert_eq!(result_seq.len(), 1500);

    // Test 3: Verify with aggregation queries too
    let stmt_agg = parse_select("SELECT COUNT(*) FROM TEST_PARALLEL WHERE value < 10");
    let result_agg = executor.execute(&stmt_agg).unwrap();

    // Should get one row with count
    assert_eq!(result_agg.len(), 1);
    assert_eq!(result_agg[0].values[0], vibesql_types::SqlValue::Integer(1500));

    // Test 4: Verify custom threshold works
    std::env::set_var("PARALLEL_THRESHOLD", "1000");
    let result_custom = executor.execute(&stmt).unwrap();
    std::env::remove_var("PARALLEL_THRESHOLD");
    assert_eq!(result_custom.len(), 1500);
}
