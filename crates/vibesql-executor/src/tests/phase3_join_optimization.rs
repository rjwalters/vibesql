//! Tests for Phase 3 Join Optimization
//!
//! Phase 3.1: Enhanced hash join selection for WHERE clause equijoins
//! Phase 3.2: Join condition reordering for optimal execution
//! Phase 3.3: Vectorized equijoin evaluation

use super::super::*;

/// Test that hash join is selected for equijoin predicates in WHERE clause
/// even when there's no ON clause.
///
/// This is Phase 3.1 - Enhanced hash join selection
#[test]
fn test_hash_join_from_where_equijoin_no_on_clause() {
    let mut db = vibesql_storage::Database::new();

    // Create table t1
    let t1_schema = vibesql_catalog::TableSchema::new(
        "t1".to_string(),
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
    db.create_table(t1_schema).unwrap();

    // Insert 5 rows
    for i in 1..=5 {
        db.insert_row(
            "t1",
            vibesql_storage::Row::new(vec![
                vibesql_types::SqlValue::Integer(i),
                vibesql_types::SqlValue::Integer(i * 10),
            ]),
        )
        .unwrap();
    }

    // Create table t2
    let t2_schema = vibesql_catalog::TableSchema::new(
        "t2".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "name".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(50) },
                false,
            ),
        ],
    );
    db.create_table(t2_schema).unwrap();

    // Insert 5 rows
    for i in 1..=5 {
        db.insert_row(
            "t2",
            vibesql_storage::Row::new(vec![
                vibesql_types::SqlValue::Integer(i),
                vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(format!("name_{}", i))),
            ]),
        )
        .unwrap();
    }

    // Query with equijoin in WHERE clause, NO ON clause
    // This should use hash join via Phase 3.1
    let executor = SelectExecutor::new(&db);
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        from: Some(vibesql_ast::FromClause::Join {
            left: Box::new(vibesql_ast::FromClause::Table {
                name: "t1".to_string(),
                alias: None,
                column_aliases: None,
            }),
            right: Box::new(vibesql_ast::FromClause::Table {
                name: "t2".to_string(),
                alias: None,
                column_aliases: None,
            }),
            join_type: vibesql_ast::JoinType::Inner,
            condition: None, // NO ON clause - equijoin is in WHERE
            natural: false,
        }),
        where_clause: Some(vibesql_ast::Expression::BinaryOp {
            left: Box::new(vibesql_ast::Expression::ColumnRef {
                table: Some("t1".to_string()),
                column: "id".to_string(),
            }),
            op: vibesql_ast::BinaryOperator::Equal,
            right: Box::new(vibesql_ast::Expression::ColumnRef {
                table: Some("t2".to_string()),
                column: "id".to_string(),
            }),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();

    // Should have 5 rows (one for each matching ID)
    assert_eq!(result.len(), 5, "Expected 5 rows from equijoin");

    // Verify data integrity - first row should have t1.id=1, t1.value=10, t2.id=1, t2.name='name_1'
    let first_row = &result[0];
    assert_eq!(first_row.values.len(), 4, "Expected 4 columns");
    assert_eq!(first_row.values[0], vibesql_types::SqlValue::Integer(1)); // t1.id
    assert_eq!(first_row.values[1], vibesql_types::SqlValue::Integer(10)); // t1.value
    assert_eq!(first_row.values[2], vibesql_types::SqlValue::Integer(1)); // t2.id
    assert_eq!(first_row.values[3], vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("name_1")));
    // t2.name
}

/// Test that multiple equijoins are handled correctly when hash join uses one
#[test]
fn test_hash_join_multiple_equijoins_in_where() {
    let mut db = vibesql_storage::Database::new();

    // Create table t1 with extra column
    let t1_schema = vibesql_catalog::TableSchema::new(
        "t1".to_string(),
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
    db.create_table(t1_schema).unwrap();

    for i in 1..=3 {
        db.insert_row(
            "t1",
            vibesql_storage::Row::new(vec![
                vibesql_types::SqlValue::Integer(i),
                vibesql_types::SqlValue::Integer(i * 10),
            ]),
        )
        .unwrap();
    }

    // Create table t2 with matching columns
    let t2_schema = vibesql_catalog::TableSchema::new(
        "t2".to_string(),
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
    db.create_table(t2_schema).unwrap();

    for i in 1..=3 {
        db.insert_row(
            "t2",
            vibesql_storage::Row::new(vec![
                vibesql_types::SqlValue::Integer(i),
                vibesql_types::SqlValue::Integer(i * 20),
            ]),
        )
        .unwrap();
    }

    // Query with TWO equijoins in WHERE clause
    // Hash join should use the first one and filter with the second
    let executor = SelectExecutor::new(&db);
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        from: Some(vibesql_ast::FromClause::Join {
            left: Box::new(vibesql_ast::FromClause::Table {
                name: "t1".to_string(),
                alias: None,
                column_aliases: None,
            }),
            right: Box::new(vibesql_ast::FromClause::Table {
                name: "t2".to_string(),
                alias: None,
                column_aliases: None,
            }),
            join_type: vibesql_ast::JoinType::Inner,
            condition: None,
            natural: false,
        }),
        where_clause: Some(vibesql_ast::Expression::BinaryOp {
            left: Box::new(vibesql_ast::Expression::BinaryOp {
                left: Box::new(vibesql_ast::Expression::ColumnRef {
                    table: Some("t1".to_string()),
                    column: "id".to_string(),
                }),
                op: vibesql_ast::BinaryOperator::Equal,
                right: Box::new(vibesql_ast::Expression::ColumnRef {
                    table: Some("t2".to_string()),
                    column: "id".to_string(),
                }),
            }),
            op: vibesql_ast::BinaryOperator::And,
            right: Box::new(vibesql_ast::Expression::BinaryOp {
                left: Box::new(vibesql_ast::Expression::ColumnRef {
                    table: Some("t1".to_string()),
                    column: "value".to_string(),
                }),
                op: vibesql_ast::BinaryOperator::Equal,
                right: Box::new(vibesql_ast::Expression::ColumnRef {
                    table: Some("t2".to_string()),
                    column: "value".to_string(),
                }),
            }),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();

    // t1.value = [10, 20, 30] (id 1, 2, 3)
    // t2.value = [20, 40, 60] (id 1, 2, 3)
    //
    // Both id AND value must match:
    // t1.id=t2.id AND t1.value=t2.value
    //
    // id=1: t1.value=10 vs t2.value=20 - NO
    // id=2: t1.value=20 vs t2.value=40 - NO
    // id=3: t1.value=30 vs t2.value=60 - NO
    //
    // So no rows match both conditions
    assert_eq!(result.len(), 0, "Expected 0 rows where both conditions match");
}

/// Test cascading joins with WHERE clause equijoins (simplified select5 scenario)
#[test]
fn test_cascading_joins_with_where_equijoins() {
    let mut db = vibesql_storage::Database::new();

    // Create 4 tables with 10 rows each
    for table_num in 1..=4 {
        let table_name = format!("t{}", table_num);
        let schema = vibesql_catalog::TableSchema::new(
            table_name.clone(),
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

        // Insert 10 rows per table
        for row_num in 1..=10 {
            db.insert_row(
                &table_name,
                vibesql_storage::Row::new(vec![
                    vibesql_types::SqlValue::Integer(row_num as i64),
                    vibesql_types::SqlValue::Integer(row_num as i64 + table_num as i64),
                ]),
            )
            .unwrap();
        }
    }

    // Query with cascading equijoins in WHERE
    // Without Phase 3.1, this would build: 10*10=100 rows, then 100*10=1000, etc.
    // With Phase 3.1 hash join:
    //   t1 (10 rows) JOIN t2 ON t1.id=t2.id → 10 rows
    //   result (10 rows) JOIN t3 ON t2.id=t3.id → 10 rows
    //   result (10 rows) JOIN t4 ON t3.id=t4.id → 10 rows
    let executor = SelectExecutor::new(&db);

    // Build nested join manually
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        from: Some(vibesql_ast::FromClause::Join {
            left: Box::new(vibesql_ast::FromClause::Join {
                left: Box::new(vibesql_ast::FromClause::Join {
                    left: Box::new(vibesql_ast::FromClause::Table {
                        name: "t1".to_string(),
                        alias: None,
                        column_aliases: None,
                    }),
                    right: Box::new(vibesql_ast::FromClause::Table {
                        name: "t2".to_string(),
                        alias: None,
                        column_aliases: None,
                    }),
                    join_type: vibesql_ast::JoinType::Inner,
                    condition: None,
                    natural: false,
                }),
                right: Box::new(vibesql_ast::FromClause::Table {
                    name: "t3".to_string(),
                    alias: None,
                    column_aliases: None,
                }),
                join_type: vibesql_ast::JoinType::Inner,
                condition: None,
                natural: false,
            }),
            right: Box::new(vibesql_ast::FromClause::Table {
                name: "t4".to_string(),
                alias: None,
                column_aliases: None,
            }),
            join_type: vibesql_ast::JoinType::Inner,
            condition: None,
            natural: false,
        }),
        where_clause: Some(vibesql_ast::Expression::BinaryOp {
            left: Box::new(vibesql_ast::Expression::BinaryOp {
                left: Box::new(vibesql_ast::Expression::BinaryOp {
                    left: Box::new(vibesql_ast::Expression::ColumnRef {
                        table: Some("t1".to_string()),
                        column: "id".to_string(),
                    }),
                    op: vibesql_ast::BinaryOperator::Equal,
                    right: Box::new(vibesql_ast::Expression::ColumnRef {
                        table: Some("t2".to_string()),
                        column: "id".to_string(),
                    }),
                }),
                op: vibesql_ast::BinaryOperator::And,
                right: Box::new(vibesql_ast::Expression::BinaryOp {
                    left: Box::new(vibesql_ast::Expression::ColumnRef {
                        table: Some("t2".to_string()),
                        column: "id".to_string(),
                    }),
                    op: vibesql_ast::BinaryOperator::Equal,
                    right: Box::new(vibesql_ast::Expression::ColumnRef {
                        table: Some("t3".to_string()),
                        column: "id".to_string(),
                    }),
                }),
            }),
            op: vibesql_ast::BinaryOperator::And,
            right: Box::new(vibesql_ast::Expression::BinaryOp {
                left: Box::new(vibesql_ast::Expression::ColumnRef {
                    table: Some("t3".to_string()),
                    column: "id".to_string(),
                }),
                op: vibesql_ast::BinaryOperator::Equal,
                right: Box::new(vibesql_ast::Expression::ColumnRef {
                    table: Some("t4".to_string()),
                    column: "id".to_string(),
                }),
            }),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();

    // Should have 10 rows (one for each matching ID 1-10)
    assert_eq!(result.len(), 10, "Expected 10 rows from cascading equijoins");

    // Verify first row structure
    let first_row = &result[0];
    assert_eq!(first_row.values.len(), 8, "Expected 8 columns (4 tables × 2 cols each)");
}

/// Test that Phase 3.1 doesn't break when there's an ON clause
#[test]
fn test_hash_join_with_on_clause_and_where_equijoins() {
    let mut db = vibesql_storage::Database::new();

    let t1_schema = vibesql_catalog::TableSchema::new(
        "t1".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "key".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
        ],
    );
    db.create_table(t1_schema).unwrap();

    for i in 1..=3 {
        db.insert_row(
            "t1",
            vibesql_storage::Row::new(vec![
                vibesql_types::SqlValue::Integer(i),
                vibesql_types::SqlValue::Integer(i * 10),
            ]),
        )
        .unwrap();
    }

    let t2_schema = vibesql_catalog::TableSchema::new(
        "t2".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "key".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
        ],
    );
    db.create_table(t2_schema).unwrap();

    for i in 1..=3 {
        db.insert_row(
            "t2",
            vibesql_storage::Row::new(vec![
                vibesql_types::SqlValue::Integer(i),
                vibesql_types::SqlValue::Integer(i * 10),
            ]),
        )
        .unwrap();
    }

    // Query with BOTH ON clause and WHERE equijoin
    // Should prefer ON clause for hash join
    let executor = SelectExecutor::new(&db);
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        from: Some(vibesql_ast::FromClause::Join {
            left: Box::new(vibesql_ast::FromClause::Table {
                name: "t1".to_string(),
                alias: None,
                column_aliases: None,
            }),
            right: Box::new(vibesql_ast::FromClause::Table {
                name: "t2".to_string(),
                alias: None,
                column_aliases: None,
            }),
            join_type: vibesql_ast::JoinType::Inner,
            condition: Some(vibesql_ast::Expression::BinaryOp {
                left: Box::new(vibesql_ast::Expression::ColumnRef {
                    table: Some("t1".to_string()),
                    column: "id".to_string(),
                }),
                op: vibesql_ast::BinaryOperator::Equal,
                right: Box::new(vibesql_ast::Expression::ColumnRef {
                    table: Some("t2".to_string()),
                    column: "id".to_string(),
                }),
            }),
            natural: false,
        }),
        where_clause: Some(vibesql_ast::Expression::BinaryOp {
            left: Box::new(vibesql_ast::Expression::ColumnRef {
                table: Some("t1".to_string()),
                column: "key".to_string(),
            }),
            op: vibesql_ast::BinaryOperator::Equal,
            right: Box::new(vibesql_ast::Expression::ColumnRef {
                table: Some("t2".to_string()),
                column: "key".to_string(),
            }),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();

    // Should have 3 rows (all rows match on both conditions)
    assert_eq!(result.len(), 3, "Expected 3 rows");
}

/// Test that multi-column hash join works with composite keys
/// This is the key test for issue #2842 - TPC-H Q3, Q7, Q10 optimization
#[test]
fn test_multi_column_hash_join_composite_keys() {
    let mut db = vibesql_storage::Database::new();

    // Create sales table with (region_id, product_id)
    let sales_schema = vibesql_catalog::TableSchema::new(
        "sales".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "region_id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "product_id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "amount".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
        ],
    );
    db.create_table(sales_schema).unwrap();

    // Insert sales data
    for region in 1..=3 {
        for product in 1..=4 {
            db.insert_row(
                "sales",
                vibesql_storage::Row::new(vec![
                    vibesql_types::SqlValue::Integer(region),
                    vibesql_types::SqlValue::Integer(product),
                    vibesql_types::SqlValue::Integer(region * 100 + product),
                ]),
            )
            .unwrap();
        }
    }

    // Create inventory table with (region_id, product_id)
    let inventory_schema = vibesql_catalog::TableSchema::new(
        "inventory".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "region_id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "product_id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "stock".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
        ],
    );
    db.create_table(inventory_schema).unwrap();

    // Insert inventory data (only some combinations exist)
    // region 1, products 1,2
    // region 2, products 2,3
    // region 3, products 3,4
    for (region, products) in [(1, vec![1, 2]), (2, vec![2, 3]), (3, vec![3, 4])] {
        for product in products {
            db.insert_row(
                "inventory",
                vibesql_storage::Row::new(vec![
                    vibesql_types::SqlValue::Integer(region),
                    vibesql_types::SqlValue::Integer(product),
                    vibesql_types::SqlValue::Integer(region * 10 + product),
                ]),
            )
            .unwrap();
        }
    }

    // Query with composite key join: region_id AND product_id
    let executor = SelectExecutor::new(&db);
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        from: Some(vibesql_ast::FromClause::Join {
            left: Box::new(vibesql_ast::FromClause::Table {
                name: "sales".to_string(),
                alias: None,
                column_aliases: None,
            }),
            right: Box::new(vibesql_ast::FromClause::Table {
                name: "inventory".to_string(),
                alias: None,
                column_aliases: None,
            }),
            join_type: vibesql_ast::JoinType::Inner,
            // ON clause with composite key
            condition: Some(vibesql_ast::Expression::BinaryOp {
                left: Box::new(vibesql_ast::Expression::BinaryOp {
                    left: Box::new(vibesql_ast::Expression::ColumnRef {
                        table: Some("sales".to_string()),
                        column: "region_id".to_string(),
                    }),
                    op: vibesql_ast::BinaryOperator::Equal,
                    right: Box::new(vibesql_ast::Expression::ColumnRef {
                        table: Some("inventory".to_string()),
                        column: "region_id".to_string(),
                    }),
                }),
                op: vibesql_ast::BinaryOperator::And,
                right: Box::new(vibesql_ast::Expression::BinaryOp {
                    left: Box::new(vibesql_ast::Expression::ColumnRef {
                        table: Some("sales".to_string()),
                        column: "product_id".to_string(),
                    }),
                    op: vibesql_ast::BinaryOperator::Equal,
                    right: Box::new(vibesql_ast::Expression::ColumnRef {
                        table: Some("inventory".to_string()),
                        column: "product_id".to_string(),
                    }),
                }),
            }),
            natural: false,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();

    // Expected matches:
    // (1,1), (1,2), (2,2), (2,3), (3,3), (3,4) = 6 matches
    assert_eq!(result.len(), 6, "Expected 6 rows from composite key join");

    // Verify row structure (6 columns total)
    for row in result.iter() {
        assert_eq!(row.values.len(), 6, "Expected 6 columns");
        // Verify the join keys match
        assert_eq!(row.values[0], row.values[3], "region_id should match"); // sales.region_id = inventory.region_id
        assert_eq!(row.values[1], row.values[4], "product_id should match"); // sales.product_id = inventory.product_id
    }
}

/// Test star join pattern that mimics select5.test memory issues
///
/// This demonstrates the REAL value of join reordering for select5.test:
/// - Star pattern: central hub table with multiple spokes
/// - Without reordering: Cartesian products create millions of intermediate rows
/// - With reordering: Hub-first ordering keeps intermediate results small
///
/// **select5.test problem**: 64 tables, each with 10 rows, all joining to a central table.
/// Without optimization: 10^64 intermediate rows (memory exhaustion!)
/// With BFS reordering: ~640 intermediate rows (manageable)
#[test]
fn test_star_join_select5_pattern() {
    let mut db = vibesql_storage::Database::new();

    // Create 6 tables simulating select5.test pattern (scaled down from 64)
    // t1 is the hub - all other tables join to it
    // Each table has 10 rows (same as select5.test)
    for table_num in 1..=6 {
        let table_name = format!("t{}", table_num);
        let schema = vibesql_catalog::TableSchema::new(
            table_name.clone(),
            vec![
                vibesql_catalog::ColumnSchema::new(
                    "id".to_string(),
                    vibesql_types::DataType::Integer,
                    false,
                ),
                vibesql_catalog::ColumnSchema::new(
                    format!("val{}", table_num),
                    vibesql_types::DataType::Integer,
                    false,
                ),
            ],
        );
        db.create_table(schema).unwrap();

        // 10 rows per table (select5.test has 10 rows)
        for row_num in 1..=10 {
            db.insert_row(
                &table_name,
                vibesql_storage::Row::new(vec![
                    vibesql_types::SqlValue::Integer(row_num as i64),
                    vibesql_types::SqlValue::Integer(row_num as i64 * 10 + table_num as i64),
                ]),
            )
            .unwrap();
        }
    }

    let executor = SelectExecutor::new(&db);

    // Star join: all tables join to t1 (the hub)
    // SELECT * FROM t1, t2, t3, t4, t5, t6
    // WHERE t1.id = t2.id
    //   AND t1.id = t3.id
    //   AND t1.id = t4.id
    //   AND t1.id = t5.id
    //   AND t1.id = t6.id
    //
    // WITHOUT reordering (left-to-right):
    //   t1 × t2 = 100 rows (no condition applied yet!)
    //   × t3 = 1,000 rows
    //   × t4 = 10,000 rows
    //   × t5 = 100,000 rows ⚠️
    //   × t6 = 1,000,000 rows ⚠️⚠️
    //   WHERE filters to 10 rows (too late!)
    //
    // WITH BFS reordering (t1 first as hub):
    //   t1 (10 rows)
    //   JOIN t2 ON t1.id=t2.id → 10 rows
    //   JOIN t3 ON t1.id=t3.id → 10 rows
    //   JOIN t4 ON t1.id=t4.id → 10 rows
    //   JOIN t5 ON t1.id=t5.id → 10 rows
    //   JOIN t6 ON t1.id=t6.id → 10 rows
    //   Total: 50 intermediate rows (22,000× reduction!)

    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        from: Some(vibesql_ast::FromClause::Join {
            left: Box::new(vibesql_ast::FromClause::Join {
                left: Box::new(vibesql_ast::FromClause::Join {
                    left: Box::new(vibesql_ast::FromClause::Join {
                        left: Box::new(vibesql_ast::FromClause::Join {
                            left: Box::new(vibesql_ast::FromClause::Table {
                                name: "t1".to_string(),
                                alias: None,
                                column_aliases: None,
                            }),
                            right: Box::new(vibesql_ast::FromClause::Table {
                                name: "t2".to_string(),
                                alias: None,
                                column_aliases: None,
                            }),
                            join_type: vibesql_ast::JoinType::Inner,
                            condition: None, // Star join: conditions in WHERE
                            natural: false,
                        }),
                        right: Box::new(vibesql_ast::FromClause::Table {
                            name: "t3".to_string(),
                            alias: None,
                            column_aliases: None,
                        }),
                        join_type: vibesql_ast::JoinType::Inner,
                        condition: None,
                        natural: false,
                    }),
                    right: Box::new(vibesql_ast::FromClause::Table {
                        name: "t4".to_string(),
                        alias: None,
                        column_aliases: None,
                    }),
                    join_type: vibesql_ast::JoinType::Inner,
                    condition: None,
                    natural: false,
                }),
                right: Box::new(vibesql_ast::FromClause::Table {
                    name: "t5".to_string(),
                    alias: None,
                    column_aliases: None,
                }),
                join_type: vibesql_ast::JoinType::Inner,
                condition: None,
                natural: false,
            }),
            right: Box::new(vibesql_ast::FromClause::Table {
                name: "t6".to_string(),
                alias: None,
                column_aliases: None,
            }),
            join_type: vibesql_ast::JoinType::Inner,
            condition: None,
            natural: false,
        }),
        // All joins connect to t1 (star pattern)
        where_clause: Some(vibesql_ast::Expression::BinaryOp {
            left: Box::new(vibesql_ast::Expression::BinaryOp {
                left: Box::new(vibesql_ast::Expression::BinaryOp {
                    left: Box::new(vibesql_ast::Expression::BinaryOp {
                        left: Box::new(vibesql_ast::Expression::BinaryOp {
                            left: Box::new(vibesql_ast::Expression::ColumnRef {
                                table: Some("t1".to_string()),
                                column: "id".to_string(),
                            }),
                            op: vibesql_ast::BinaryOperator::Equal,
                            right: Box::new(vibesql_ast::Expression::ColumnRef {
                                table: Some("t2".to_string()),
                                column: "id".to_string(),
                            }),
                        }),
                        op: vibesql_ast::BinaryOperator::And,
                        right: Box::new(vibesql_ast::Expression::BinaryOp {
                            left: Box::new(vibesql_ast::Expression::ColumnRef {
                                table: Some("t1".to_string()),
                                column: "id".to_string(),
                            }),
                            op: vibesql_ast::BinaryOperator::Equal,
                            right: Box::new(vibesql_ast::Expression::ColumnRef {
                                table: Some("t3".to_string()),
                                column: "id".to_string(),
                            }),
                        }),
                    }),
                    op: vibesql_ast::BinaryOperator::And,
                    right: Box::new(vibesql_ast::Expression::BinaryOp {
                        left: Box::new(vibesql_ast::Expression::ColumnRef {
                            table: Some("t1".to_string()),
                            column: "id".to_string(),
                        }),
                        op: vibesql_ast::BinaryOperator::Equal,
                        right: Box::new(vibesql_ast::Expression::ColumnRef {
                            table: Some("t4".to_string()),
                            column: "id".to_string(),
                        }),
                    }),
                }),
                op: vibesql_ast::BinaryOperator::And,
                right: Box::new(vibesql_ast::Expression::BinaryOp {
                    left: Box::new(vibesql_ast::Expression::ColumnRef {
                        table: Some("t1".to_string()),
                        column: "id".to_string(),
                    }),
                    op: vibesql_ast::BinaryOperator::Equal,
                    right: Box::new(vibesql_ast::Expression::ColumnRef {
                        table: Some("t5".to_string()),
                        column: "id".to_string(),
                    }),
                }),
            }),
            op: vibesql_ast::BinaryOperator::And,
            right: Box::new(vibesql_ast::Expression::BinaryOp {
                left: Box::new(vibesql_ast::Expression::ColumnRef {
                    table: Some("t1".to_string()),
                    column: "id".to_string(),
                }),
                op: vibesql_ast::BinaryOperator::Equal,
                right: Box::new(vibesql_ast::Expression::ColumnRef {
                    table: Some("t6".to_string()),
                    column: "id".to_string(),
                }),
            }),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();

    // Should have 10 rows (one for each id 1-10 that appears in all tables)
    assert_eq!(result.len(), 10, "Expected 10 rows from star join");

    // Verify structure: 6 tables × 2 columns = 12 columns
    let first_row = &result[0];
    assert_eq!(first_row.values.len(), 12, "Expected 12 columns (6 tables × 2 cols)");

    // Verify correctness: first row should have id=1 from all tables
    assert_eq!(first_row.values[0], vibesql_types::SqlValue::Integer(1)); // t1.id
    assert_eq!(first_row.values[2], vibesql_types::SqlValue::Integer(1)); // t2.id
    assert_eq!(first_row.values[4], vibesql_types::SqlValue::Integer(1)); // t3.id
    assert_eq!(first_row.values[6], vibesql_types::SqlValue::Integer(1)); // t4.id
    assert_eq!(first_row.values[8], vibesql_types::SqlValue::Integer(1)); // t5.id
    assert_eq!(first_row.values[10], vibesql_types::SqlValue::Integer(1)); // t6.id

    // NOTE: Without join reordering, this test would create ~1.1M intermediate rows.
    // With join reordering detecting t1 as hub and placing it first, only ~50 intermediate rows.
    // For select5.test with 64 tables: without = 10^64 rows (OOM), with = ~640 rows (success!)
}
