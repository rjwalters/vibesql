#[cfg(test)]
mod memory_tracking_tests {
    use vibesql_storage::Database;

    use crate::{
        errors::ExecutorError, limits::MAX_MEMORY_BYTES, select::executor::builder::SelectExecutor,
    };

    #[test]
    fn test_memory_limit_exceeded() {
        let db = Database::new();
        let executor = SelectExecutor::new(&db);

        // Simulate allocating 15 GB (exceeds 10 GB limit)
        let result = executor.track_memory_allocation(15 * 1024 * 1024 * 1024);

        assert!(
            matches!(result, Err(ExecutorError::MemoryLimitExceeded { .. })),
            "Expected MemoryLimitExceeded error"
        );

        if let Err(ExecutorError::MemoryLimitExceeded { used_bytes, max_bytes }) = result {
            assert_eq!(used_bytes, 15 * 1024 * 1024 * 1024);
            assert_eq!(max_bytes, MAX_MEMORY_BYTES);
        } else {
            panic!("Expected MemoryLimitExceeded variant");
        }
    }

    #[test]
    fn test_memory_allocation_below_limit() {
        let db = Database::new();
        let executor = SelectExecutor::new(&db);

        // Allocate 2 GB (below 10 GB limit)
        let result = executor.track_memory_allocation(2 * 1024 * 1024 * 1024);

        assert!(result.is_ok(), "Should allow allocation below limit");
    }

    #[test]
    fn test_memory_deallocation() {
        let db = Database::new();
        let executor = SelectExecutor::new(&db);

        // Allocate 3 GB
        executor.track_memory_allocation(3 * 1024 * 1024 * 1024).unwrap();

        // Deallocate 1 GB
        executor.track_memory_deallocation(1024 * 1024 * 1024);

        // Allocate another 3 GB (total would be 5 GB, which is below limit)
        let result = executor.track_memory_allocation(3 * 1024 * 1024 * 1024);
        assert!(result.is_ok(), "Should allow allocation when within limit");
    }

    #[test]
    fn test_memory_deallocation_underflow() {
        let db = Database::new();
        let executor = SelectExecutor::new(&db);

        // Try to deallocate more than allocated
        executor.track_memory_deallocation(100 * 1024 * 1024 * 1024);

        // Should saturate at 0, not underflow
        executor
            .track_memory_allocation(1)
            .expect("Should work with minimal allocation after underflow");
    }
}

#[cfg(test)]
mod integration_tests {
    use vibesql_storage::Database;

    use crate::{errors::ExecutorError, select::executor::builder::SelectExecutor};

    #[test]
    fn test_normal_query_within_memory_limit() {
        // Test that normal queries don't hit the memory limit
        let mut db = Database::new();

        // Create schema
        let schema = vibesql_catalog::TableSchema::new(
            "small_table".to_string(),
            vec![
                vibesql_catalog::ColumnSchema::new(
                    "id".to_string(),
                    vibesql_types::DataType::Integer,
                    false,
                ),
                vibesql_catalog::ColumnSchema::new(
                    "name".to_string(),
                    vibesql_types::DataType::Varchar { max_length: Some(100) },
                    false,
                ),
            ],
        );
        db.create_table(schema).unwrap();

        // Insert test data
        db.insert_row(
            "small_table",
            vibesql_storage::Row::new(vec![
                vibesql_types::SqlValue::Integer(1),
                vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
            ]),
        )
        .unwrap();
        db.insert_row(
            "small_table",
            vibesql_storage::Row::new(vec![
                vibesql_types::SqlValue::Integer(2),
                vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Bob")),
            ]),
        )
        .unwrap();

        let executor = SelectExecutor::new(&db);
        let stmt = vibesql_ast::SelectStmt {
            into_table: None,
            into_variables: None,
            with_clause: None,
            set_operation: None,
            distinct: false,
            select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
            from: Some(vibesql_ast::FromClause::Table {
                name: "small_table".to_string(),
                alias: None,
                column_aliases: None,
            }),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
        };

        let result = executor.execute(&stmt);
        assert!(result.is_ok(), "Normal query should succeed");

        let rows = result.unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_massive_join_caught_early() {
        // Test that joins with huge cartesian products are caught BEFORE execution
        // This verifies the join-level size checks prevent OOM
        let mut db = Database::new();

        // Create two tables
        let schema1 = vibesql_catalog::TableSchema::new(
            "t1".to_string(),
            vec![vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            )],
        );
        db.create_table(schema1).unwrap();

        let schema2 = vibesql_catalog::TableSchema::new(
            "t2".to_string(),
            vec![vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            )],
        );
        db.create_table(schema2).unwrap();

        // Insert enough rows to exceed MAX_JOIN_RESULT_ROWS (100M)
        // 10,100 x 10,100 = 102,010,000 rows (exceeds limit with minimal overhead)
        // Reduced from 15,000 to speed up test while still triggering the limit
        for i in 0..10_100 {
            db.insert_row(
                "t1",
                vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(i)]),
            )
            .unwrap();
            db.insert_row(
                "t2",
                vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(i)]),
            )
            .unwrap();
        }

        let executor = SelectExecutor::new(&db);

        // Test CROSS JOIN
        let cross_join_stmt = vibesql_ast::SelectStmt {
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
                join_type: vibesql_ast::JoinType::Cross,
                condition: None,
                natural: false,
            }),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
        };

        let result = executor.execute(&cross_join_stmt);

        assert!(
            matches!(result, Err(ExecutorError::MemoryLimitExceeded { .. })),
            "Massive CROSS JOIN should be caught before execution, got: {:?}",
            result
        );

        // Test INNER JOIN without selective condition (also cartesian-like)
        let inner_join_stmt = vibesql_ast::SelectStmt {
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
                condition: Some(vibesql_ast::Expression::Literal(
                    vibesql_types::SqlValue::Boolean(true),
                )),
                natural: false,
            }),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
        };

        let result = executor.execute(&inner_join_stmt);

        assert!(
            matches!(result, Err(ExecutorError::MemoryLimitExceeded { .. })),
            "Massive INNER JOIN should be caught before execution, got: {:?}",
            result
        );
    }
}
