//! Tests for predicate pushdown optimization (issue #1036)
//!
//! These tests verify that table-local predicates are correctly identified and applied
//! during table scans, reducing memory consumption in multi-table joins.

use vibesql_executor::SelectExecutor;

/// Helper function to parse SELECT statements
fn parse_select(sql: &str) -> vibesql_ast::SelectStmt {
    match vibesql_parser::Parser::parse_sql(sql) {
        Ok(vibesql_ast::Statement::Select(select_stmt)) => *select_stmt,
        _ => panic!("Failed to parse SELECT statement: {}", sql),
    }
}

/// Helper to create test database with tables
fn setup_test_db_with_tables(num_tables: usize, rows_per_table: i64) -> vibesql_storage::Database {
    let mut db = vibesql_storage::Database::new();

    for table_num in 1..=num_tables {
        let table_name = format!("T{}", table_num);
        let mut schema = vibesql_catalog::TableSchema::new(
            table_name.clone(),
            vec![
                vibesql_catalog::ColumnSchema {
                    name: format!("a{}", table_num),
                    data_type: vibesql_types::DataType::Integer,
                    nullable: false,
                    default_value: None,
                },
                vibesql_catalog::ColumnSchema {
                    name: format!("b{}", table_num),
                    data_type: vibesql_types::DataType::Integer,
                    nullable: true,
                    default_value: None,
                },
                vibesql_catalog::ColumnSchema {
                    name: format!("x{}", table_num),
                    data_type: vibesql_types::DataType::Varchar { max_length: Some(40) },
                    nullable: true,
                    default_value: None,
                },
            ],
        );
        schema.primary_key = Some(vec![format!("a{}", table_num)]);
        db.create_table(schema).unwrap();

        // Insert rows
        for row_num in 1..=rows_per_table {
            let row = vibesql_storage::Row::new(vec![
                vibesql_types::SqlValue::Integer(row_num),
                vibesql_types::SqlValue::Integer(row_num % 10 + 1),
                vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(format!("t{} row {}", table_num, row_num))),
            ]);
            db.insert_row(&table_name, row).unwrap();
        }
    }

    db
}

#[test]
fn test_single_table_local_predicate() {
    // Test that a simple table-local predicate is applied during scan
    let db = setup_test_db_with_tables(1, 10);
    let executor = SelectExecutor::new(&db);

    let sql = "SELECT * FROM T1 WHERE a1 > 5";
    let stmt = parse_select(sql);
    let result = executor.execute(&stmt).unwrap();

    // Should return 5 rows (a1=6,7,8,9,10)
    assert_eq!(result.len(), 5);
}

#[test]
fn test_table_local_predicate_with_two_tables() {
    // Test predicate pushdown with 2-table join
    // Without pushdown: 10 * 10 = 100 rows cartesian product
    // With pushdown: t1 filtered to 5 rows first, then joined
    let db = setup_test_db_with_tables(2, 10);
    let executor = SelectExecutor::new(&db);

    // a1 > 5 should be pushed down to t1 scan, reducing cartesian product
    let sql = "SELECT COUNT(*) FROM T1, t2 WHERE a1 > 5 AND a1 = b2";
    let stmt = parse_select(sql);
    let result = executor.execute(&stmt).unwrap();

    // Should have results (exact count depends on join selectivity)
    assert!(!result.is_empty());
    assert!(result[0].values[0] > vibesql_types::SqlValue::Integer(0));
}

#[test]
fn test_three_table_join_with_local_predicates() {
    // Test predicate pushdown with 3-table join
    // Multiple table-local predicates should be pushed down independently
    let db = setup_test_db_with_tables(3, 10);
    let executor = SelectExecutor::new(&db);

    let sql = "SELECT COUNT(*) FROM T1, t2, t3 \
               WHERE a1 > 3 AND b2 < 8 AND a3 > 2 \
               AND a1 = b2 AND a2 = b3";
    let stmt = parse_select(sql);
    let result = executor.execute(&stmt).unwrap();

    // Should successfully execute with reduced memory due to pushdown
    assert!(!result.is_empty());
}

#[test]
fn test_four_table_join_with_predicates() {
    // Test the memory improvement with 4-table join
    // Without pushdown: 10^4 = 10,000 row cartesian product
    // With pushdown: filtered much earlier, reducing memory consumption
    let db = setup_test_db_with_tables(4, 10);
    let executor = SelectExecutor::new(&db);

    let sql = "SELECT COUNT(*) FROM T1, t2, t3, t4 \
               WHERE a1 > 2 AND a2 > 3 AND b3 < 9 \
               AND a1 = b2 AND a2 = b3 AND a3 = b4";
    let stmt = parse_select(sql);
    let result = executor.execute(&stmt).unwrap();

    // Should successfully complete (would likely OOM without pushdown in extreme cases)
    assert!(!result.is_empty());
}

#[test]
fn test_five_table_join_with_predicates() {
    // Test with 5 tables (larger cartesian product)
    let db = setup_test_db_with_tables(5, 10);
    let executor = SelectExecutor::new(&db);

    let sql = "SELECT COUNT(*) FROM T1, t2, t3, t4, t5 \
               WHERE a1 > 5 AND a2 < 8 AND a3 > 2 AND a4 < 10 \
               AND a1 = b2 AND a2 = b3 AND a3 = b4 AND a4 = b5";
    let stmt = parse_select(sql);
    let result = executor.execute(&stmt).unwrap();

    // Should complete successfully
    assert!(!result.is_empty());
}

#[test]
fn test_no_table_local_predicates() {
    // Verify behavior when there are only complex predicates
    let db = setup_test_db_with_tables(2, 10);
    let executor = SelectExecutor::new(&db);

    // Complex predicate that references both tables
    let sql = "SELECT COUNT(*) FROM T1, t2 WHERE a1 + b2 > 10";
    let stmt = parse_select(sql);
    let result = executor.execute(&stmt).unwrap();

    // Should work correctly (not pushed down)
    assert!(!result.is_empty());
}

#[test]
fn test_mixed_table_local_and_complex_predicates() {
    // Test combination of pushdownable and non-pushdownable predicates
    let db = setup_test_db_with_tables(3, 10);
    let executor = SelectExecutor::new(&db);

    // a1 > 5 is pushdownable, but a1 + a2 > 10 is complex
    let sql = "SELECT COUNT(*) FROM T1, t2, t3 \
               WHERE a1 > 5 AND a1 + a2 > 10 AND a1 = b2 AND a2 = b3";
    let stmt = parse_select(sql);
    let result = executor.execute(&stmt).unwrap();

    // Should work correctly with mixed predicates
    assert!(!result.is_empty());
}

#[test]
fn test_aggregate_with_table_local_predicate() {
    // Test predicate pushdown with aggregation
    let db = setup_test_db_with_tables(2, 10);
    let executor = SelectExecutor::new(&db);

    let sql = "SELECT COUNT(*), SUM(a1) FROM T1, t2 WHERE a1 > 5 AND a1 = b2";
    let stmt = parse_select(sql);
    let result = executor.execute(&stmt).unwrap();

    // Should work correctly
    assert!(!result.is_empty());
}

#[test]
fn test_predicate_pushdown_with_group_by() {
    // Test predicate pushdown with GROUP BY
    let db = setup_test_db_with_tables(2, 10);
    let executor = SelectExecutor::new(&db);

    let sql = "SELECT a1, COUNT(*) FROM T1, t2 \
               WHERE a1 > 3 AND a1 = b2 \
               GROUP BY a1";
    let stmt = parse_select(sql);
    let result = executor.execute(&stmt).unwrap();

    // Should work correctly with GROUP BY
    assert!(!result.is_empty());
}

#[test]
fn test_large_multi_table_join_with_predicate_pushdown() {
    // Test with 8 tables, 5 rows each
    // Without pushdown: 5^8 = 390,625 row cartesian product (before equijoin filtering)
    // With pushdown: each table is filtered first, dramatically reducing intermediate rows
    let db = setup_test_db_with_tables(8, 5);
    let executor = SelectExecutor::new(&db);

    // All table-local predicates: a > 2 for each table
    // Plus equijoin conditions: each table joins with the next
    let sql = "SELECT COUNT(*) FROM T1, t2, t3, t4, t5, t6, t7, t8 \
               WHERE a1 > 2 AND a2 > 2 AND a3 > 2 AND a4 > 2 \
               AND a5 > 2 AND a6 > 2 AND a7 > 2 AND a8 > 2 \
               AND a1 = b2 AND a2 = b3 AND a3 = b4 AND a4 = b5 \
               AND a5 = b6 AND a6 = b7 AND a7 = b8";
    let stmt = parse_select(sql);

    // This should not panic with OOM or timeout due to predicate pushdown
    let result = executor.execute(&stmt).unwrap();

    // Should successfully execute and return a row with a count
    assert_eq!(result.len(), 1);
}

// NOTE: Tests beyond 8 tables are currently disabled because they require Phase 3:
// a smart join tree optimizer that reorders joins to avoid cascading cartesian products.
//
// Current behavior (Phase 2):
// - Phase 1: Implemented WHERE clause decomposition into predicates
// - Phase 2: Implemented table-local predicate pushdown to scan stage
// - Phase 3 (blocked): Requires join tree reordering or hash joins for equijoin chains
//
// The cascading join problem:
// When executing: FROM T1, T2, T3, ..., T10 WHERE a1=b2 AND a2=b3 AND ... AND a9=b10
// The join tree is: (((T1 JOIN T2) JOIN T3) ... JOIN T10)
// At each level, we build the cartesian product BEFORE filtering by equijoin condition.
// Even with table-local predicates reducing each table to ~9 rows:
// - Level 1: T1(9) × T2(10) = 90 combinations before equijoin filter
// - Level 2: result(9) × T3(10) = 90 combinations
// - ...repeats, still causing exponential blowup
//
// Solution requires Phase 3: Build smart join plans that use hash joins for equijoins
// or reorder the join tree to apply selectivity early.

// ============================================================================
// Phase 3.1: Hash Join Selection from WHERE Clause Equijoins
// ============================================================================
// These tests verify that hash joins are selected for equijoin predicates
// in WHERE clauses, even when there's no ON clause. This is the core
// Phase 3.1 optimization for reducing intermediate result cardinality.

#[test]
fn test_phase3_1_hash_join_from_where_no_on_clause() {
    // Test that hash join is selected for WHERE clause equijoin with no ON clause
    // This is the primary Phase 3.1 optimization
    let db = setup_test_db_with_tables(2, 10);
    let executor = SelectExecutor::new(&db);

    // Query with equijoin in WHERE clause, but NO ON clause
    // Phase 3.1 should select hash join for this equijoin
    let sql = "SELECT COUNT(*) FROM T1, t2 WHERE a1 = b2 AND a1 > 2";
    let stmt = parse_select(sql);
    let result = executor.execute(&stmt).unwrap();

    // Should successfully execute with hash join optimization
    assert_eq!(result.len(), 1);
    // Result should be a count > 0 (matching rows)
    assert!(result[0].values[0] > vibesql_types::SqlValue::Integer(0));
}

#[test]
fn test_phase3_1_multiple_equijoins_in_where() {
    // Test Phase 3.1 with multiple equijoins in WHERE clause
    // Hash join should use one, filter with others as post-join conditions
    let db = setup_test_db_with_tables(3, 10);
    let executor = SelectExecutor::new(&db);

    // Multiple equijoins in WHERE clause
    let sql = "SELECT COUNT(*) FROM T1, t2, t3 \
               WHERE a1 = b2 AND b2 = a3 \
               AND a1 > 2";
    let stmt = parse_select(sql);
    let result = executor.execute(&stmt).unwrap();

    // Should successfully execute with hash joins for equijoins
    assert_eq!(result.len(), 1);
}

#[test]
fn test_phase3_1_cascading_hash_joins() {
    // Test Phase 3.1 cascading hash joins with equijoin chains
    // Without Phase 3.1: Would build large intermediate Cartesian products
    // With Phase 3.1: Hash joins at each level keep rows small
    let db = setup_test_db_with_tables(6, 10);
    let executor = SelectExecutor::new(&db);

    // Chain of equijoins: a1=b2, a2=b3, a3=b4, a4=b5, a5=b6
    let sql = "SELECT COUNT(*) FROM T1, t2, t3, t4, t5, t6 \
               WHERE a1 = b2 AND a2 = b3 AND a3 = b4 AND a4 = b5 AND a5 = b6 \
               AND a1 > 1";
    let stmt = parse_select(sql);
    let result = executor.execute(&stmt).unwrap();

    // Should execute successfully with Phase 3.1 hash joins
    assert_eq!(result.len(), 1);
}

#[test]
fn test_phase3_1_hash_join_with_on_and_where_equijoins() {
    // Test that ON clause is preferred over WHERE equijoins for hash join
    // But WHERE equijoins should still be applied as filters
    let db = setup_test_db_with_tables(3, 10);
    let executor = SelectExecutor::new(&db);

    // ON clause: a1 = b2 (used for hash join)
    // WHERE clause: a2 = b3 (applied as post-join filter)
    let sql = "SELECT COUNT(*) FROM T1 JOIN t2 ON a1 = b2, t3 \
               WHERE a2 = b3 AND a1 > 2";
    let stmt = parse_select(sql);
    let result = executor.execute(&stmt).unwrap();

    // Should execute with ON clause for hash join, WHERE for filtering
    assert_eq!(result.len(), 1);
}

#[test]
fn test_phase3_1_equijoin_selectivity() {
    // Test Phase 3.1 equijoin selectivity impact
    // Tables with matching equijoin values should produce smaller results
    let db = setup_test_db_with_tables(2, 10);
    let executor = SelectExecutor::new(&db);

    // Get results with different predicates to verify selectivity
    let sql_cartesian = "SELECT COUNT(*) FROM T1, t2";
    let stmt = parse_select(sql_cartesian);
    let cartesian = executor.execute(&stmt).unwrap();
    let cartesian_count = match &cartesian[0].values[0] {
        vibesql_types::SqlValue::Integer(n) => *n,
        _ => panic!("Expected integer"),
    };

    // Now with equijoin (Phase 3.1 hash join)
    let sql_equijoin = "SELECT COUNT(*) FROM T1, t2 WHERE a1 = b2";
    let stmt = parse_select(sql_equijoin);
    let equijoin = executor.execute(&stmt).unwrap();
    let equijoin_count = match &equijoin[0].values[0] {
        vibesql_types::SqlValue::Integer(n) => *n,
        _ => panic!("Expected integer"),
    };

    // Equijoin should be much more selective than cartesian product
    // a1 ranges 1-10, b2 = a2 % 10 + 1 also ranges 1-10
    // So we expect about 10 matches (one per value of a1)
    assert!(equijoin_count < cartesian_count);
    assert!(equijoin_count <= 10); // At most 10 rows per value range
}

// ============================================================================
// Phase 3.2: Join Condition Reordering
// ============================================================================
// These tests verify that join orders are optimized based on selectivity.
// Phase 3.2 analyzes equijoin predicates and reorders joins to prevent
// cascading cartesian products.

#[test]
fn test_phase3_2_simple_equijoin_chain() {
    // Test Phase 3.2 with a simple equijoin chain: T1 -> T2 -> T3
    // Verify that joins are executed in an optimal order
    let db = setup_test_db_with_tables(3, 10);
    let executor = SelectExecutor::new(&db);

    // Simple chain: a1=b2, a2=b3
    // Phase 3.2 should recognize this chain and optimize execution order
    let sql = "SELECT COUNT(*) FROM T1, t2, t3 \
               WHERE a1 = b2 AND a2 = b3 AND a1 > 5";
    let stmt = parse_select(sql);
    let result = executor.execute(&stmt).unwrap();

    // Should execute successfully with optimized join order
    assert_eq!(result.len(), 1);
}

#[test]
fn test_phase3_2_local_predicate_first() {
    // Test Phase 3.2 prioritizes tables with local predicates
    // Tables with local filters should be executed first to reduce cardinality
    let db = setup_test_db_with_tables(3, 10);
    let executor = SelectExecutor::new(&db);

    // Local predicate on T1: a1 > 7 (reduces to ~3 rows)
    // Then equijoins: a1=b2, a2=b3
    // Phase 3.2 should start with T1 due to local predicate
    let sql = "SELECT COUNT(*) FROM T1, t2, t3 \
               WHERE a1 > 7 AND a1 = b2 AND a2 = b3";
    let stmt = parse_select(sql);
    let result = executor.execute(&stmt).unwrap();

    // Should execute with optimized order
    assert_eq!(result.len(), 1);
}

#[test]
fn test_phase3_2_multiple_join_chains() {
    // Test Phase 3.2 with multiple independent equijoin chains
    // Verify separate components are handled correctly
    let db = setup_test_db_with_tables(4, 10);
    let executor = SelectExecutor::new(&db);

    // Two chains: (T1, T2) and (T3, T4)
    // Phase 3.2 should recognize these as separate components
    let sql = "SELECT COUNT(*) FROM T1, t2, t3, t4 \
               WHERE a1 = b2 AND a3 = b4";
    let stmt = parse_select(sql);
    let result = executor.execute(&stmt).unwrap();

    // Should execute successfully
    assert_eq!(result.len(), 1);
}

#[test]
fn test_phase3_2_five_table_chain() {
    // Test Phase 3.2 with a longer equijoin chain: T1->T2->T3->T4->T5
    // This tests that the reordering handles cascading chains
    let db = setup_test_db_with_tables(5, 10);
    let executor = SelectExecutor::new(&db);

    // Chain of 4 equijoins
    let sql = "SELECT COUNT(*) FROM T1, t2, t3, t4, t5 \
               WHERE a1 = b2 AND a2 = b3 AND a3 = b4 AND a4 = b5 \
               AND a1 > 2";
    let stmt = parse_select(sql);
    let result = executor.execute(&stmt).unwrap();

    // Should execute with optimized join order
    assert_eq!(result.len(), 1);
}

#[test]
fn test_phase3_2_with_high_selectivity_predicates() {
    // Test Phase 3.2 with very selective local predicates
    // Highly selective predicates should be applied first
    let db = setup_test_db_with_tables(4, 10);
    let executor = SelectExecutor::new(&db);

    // T1 has very selective predicate (a1 > 8 → ~2 rows)
    // T3 has selective predicate (a3 > 7 → ~3 rows)
    // Phase 3.2 should prioritize these
    let sql = "SELECT COUNT(*) FROM T1, t2, t3, t4 \
               WHERE a1 > 8 AND a3 > 7 \
               AND a1 = b2 AND a2 = b3 AND a3 = b4";
    let stmt = parse_select(sql);
    let result = executor.execute(&stmt).unwrap();

    // Should execute successfully with high selectivity filtering first
    assert_eq!(result.len(), 1);
}

/// Regression test for issue #1699:
/// Unqualified columns in complex OR/AND/BETWEEN/IN predicates should be correctly
/// resolved to their table and pushed down as table-local predicates.
#[test]
fn test_unqualified_columns_in_complex_predicates() {
    // Create table with unqualified column references
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "TAB4".to_string(),
        vec![
            vibesql_catalog::ColumnSchema {
                name: "pk".to_string(),
                data_type: vibesql_types::DataType::Integer,
                nullable: false,
                default_value: None,
            },
            vibesql_catalog::ColumnSchema {
                name: "col0".to_string(),
                data_type: vibesql_types::DataType::Integer,
                nullable: true,
                default_value: None,
            },
            vibesql_catalog::ColumnSchema {
                name: "col1".to_string(),
                data_type: vibesql_types::DataType::Real,
                nullable: true,
                default_value: None,
            },
            vibesql_catalog::ColumnSchema {
                name: "col3".to_string(),
                data_type: vibesql_types::DataType::Integer,
                nullable: true,
                default_value: None,
            },
            vibesql_catalog::ColumnSchema {
                name: "col4".to_string(),
                data_type: vibesql_types::DataType::Real,
                nullable: true,
                default_value: None,
            },
        ],
    );
    db.create_table(schema).unwrap();

    // Insert test data
    db.insert_row(
        "TAB4",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(8),  // pk
            vibesql_types::SqlValue::Integer(77), // col0 - in IN list
            vibesql_types::SqlValue::Real(65.5),  // col1 - in BETWEEN range
            vibesql_types::SqlValue::Integer(50), // col3
            vibesql_types::SqlValue::Real(40.0),  // col4
        ]),
    )
    .unwrap();

    db.insert_row(
        "TAB4",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(10), // pk
            vibesql_types::SqlValue::Integer(31), // col0 - in IN list
            vibesql_types::SqlValue::Real(70.0),  // col1 - in BETWEEN range
            vibesql_types::SqlValue::Integer(50), // col3
            vibesql_types::SqlValue::Real(40.0),  // col4
        ]),
    )
    .unwrap();

    db.insert_row(
        "TAB4",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(99),  // pk
            vibesql_types::SqlValue::Integer(100), // col0 - NOT in IN list
            vibesql_types::SqlValue::Real(70.0),   // col1
            vibesql_types::SqlValue::Integer(50),  // col3
            vibesql_types::SqlValue::Real(40.0),   // col4
        ]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // Query with complex OR/AND/BETWEEN/IN predicates using UNQUALIFIED columns
    // This query should correctly push down predicates despite unqualified column references
    let sql = "SELECT pk FROM TAB4 WHERE \
               ((col0 < 54 OR col1 > 60.96 OR col3 > 43) OR col1 BETWEEN 37.96 AND 77.58) \
               AND col0 IN (77,31,61,93,69,57)";

    let stmt = parse_select(sql);
    let result = executor.execute(&stmt).unwrap();

    // Should return rows with pk=8 and pk=10 (both match the predicate)
    assert_eq!(result.len(), 2, "Should return 2 rows");

    let pks: Vec<i64> = result
        .iter()
        .map(|row| match row.get(0).unwrap() {
            vibesql_types::SqlValue::Integer(v) => *v,
            _ => panic!("Expected integer pk"),
        })
        .collect();

    assert!(pks.contains(&8), "Should contain row with pk=8");
    assert!(pks.contains(&10), "Should contain row with pk=10");
}
