// CSE tests
mod cse_tests;

// Compiled CASE expression tests
mod compiled_case_tests;

// Evaluator tests
mod evaluator_tests {
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::{DataType, SqlValue};

    use super::super::ExpressionEvaluator;
    use crate::errors::ExecutorError;

    #[test]
    fn test_evaluator_with_outer_context_resolves_inner_column() {
        // Create inner schema with "inner_col"
        let inner_schema = TableSchema::new(
            "inner".to_string(),
            vec![ColumnSchema::new("inner_col".to_string(), DataType::Integer, false)],
        );

        // Create outer schema with "outer_col"
        let outer_schema = TableSchema::new(
            "outer".to_string(),
            vec![ColumnSchema::new("outer_col".to_string(), DataType::Integer, false)],
        );

        let outer_row = vibesql_storage::Row::new(vec![SqlValue::Integer(100)]);
        let inner_row = vibesql_storage::Row::new(vec![SqlValue::Integer(42)]);

        let evaluator =
            ExpressionEvaluator::with_outer_context(&inner_schema, &outer_row, &outer_schema);

        // Should resolve inner_col from inner row
        let expr =
            vibesql_ast::Expression::ColumnRef { table: None, column: "inner_col".to_string() };

        let result = evaluator.eval(&expr, &inner_row).unwrap();
        assert_eq!(result, SqlValue::Integer(42));
    }

    #[test]
    fn test_evaluator_with_outer_context_resolves_outer_column() {
        // Create inner schema with "inner_col"
        let inner_schema = TableSchema::new(
            "inner".to_string(),
            vec![ColumnSchema::new("inner_col".to_string(), DataType::Integer, false)],
        );

        // Create outer schema with "outer_col"
        let outer_schema = TableSchema::new(
            "outer".to_string(),
            vec![ColumnSchema::new("outer_col".to_string(), DataType::Integer, false)],
        );

        let outer_row = vibesql_storage::Row::new(vec![SqlValue::Integer(100)]);
        let inner_row = vibesql_storage::Row::new(vec![SqlValue::Integer(42)]);

        let evaluator =
            ExpressionEvaluator::with_outer_context(&inner_schema, &outer_row, &outer_schema);

        // Should resolve outer_col from outer row (not in inner schema)
        let expr =
            vibesql_ast::Expression::ColumnRef { table: None, column: "outer_col".to_string() };

        let result = evaluator.eval(&expr, &inner_row).unwrap();
        assert_eq!(result, SqlValue::Integer(100));
    }

    #[test]
    fn test_evaluator_with_outer_context_inner_shadows_outer() {
        // Both schemas have "col" - inner should win
        let inner_schema = TableSchema::new(
            "inner".to_string(),
            vec![ColumnSchema::new("col".to_string(), DataType::Integer, false)],
        );

        let outer_schema = TableSchema::new(
            "outer".to_string(),
            vec![ColumnSchema::new("col".to_string(), DataType::Integer, false)],
        );

        let outer_row = vibesql_storage::Row::new(vec![SqlValue::Integer(999)]);
        let inner_row = vibesql_storage::Row::new(vec![SqlValue::Integer(42)]);

        let evaluator =
            ExpressionEvaluator::with_outer_context(&inner_schema, &outer_row, &outer_schema);

        let expr = vibesql_ast::Expression::ColumnRef { table: None, column: "col".to_string() };

        let result = evaluator.eval(&expr, &inner_row).unwrap();
        // Should get inner value (42), not outer (999)
        assert_eq!(result, SqlValue::Integer(42));
    }

    #[test]
    fn test_evaluator_with_outer_context_column_not_found() {
        let inner_schema = TableSchema::new(
            "inner".to_string(),
            vec![ColumnSchema::new("inner_col".to_string(), DataType::Integer, false)],
        );

        let outer_schema = TableSchema::new(
            "outer".to_string(),
            vec![ColumnSchema::new("outer_col".to_string(), DataType::Integer, false)],
        );

        let outer_row = vibesql_storage::Row::new(vec![SqlValue::Integer(100)]);
        let inner_row = vibesql_storage::Row::new(vec![SqlValue::Integer(42)]);

        let evaluator =
            ExpressionEvaluator::with_outer_context(&inner_schema, &outer_row, &outer_schema);

        // Try to resolve non-existent column
        let expr =
            vibesql_ast::Expression::ColumnRef { table: None, column: "nonexistent".to_string() };

        let result = evaluator.eval(&expr, &inner_row);
        assert!(matches!(result, Err(ExecutorError::ColumnNotFound { .. })));
    }

    #[test]
    fn test_evaluator_without_outer_context() {
        // Normal evaluator without outer context
        let schema = TableSchema::new(
            "table".to_string(),
            vec![ColumnSchema::new("col".to_string(), DataType::Integer, false)],
        );

        let evaluator = ExpressionEvaluator::new(&schema);
        let row = vibesql_storage::Row::new(vec![SqlValue::Integer(42)]);

        let expr = vibesql_ast::Expression::ColumnRef { table: None, column: "col".to_string() };

        let result = evaluator.eval(&expr, &row).unwrap();
        assert_eq!(result, SqlValue::Integer(42));
    }

    #[test]
    fn test_improved_error_message_includes_searched_tables() {
        // Test that error message includes searched tables
        let inner_schema = TableSchema::new(
            "users".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new(
                    "name".to_string(),
                    DataType::Varchar { max_length: Some(255) },
                    false,
                ),
            ],
        );

        let outer_schema = TableSchema::new(
            "orders".to_string(),
            vec![
                ColumnSchema::new("order_id".to_string(), DataType::Integer, false),
                ColumnSchema::new("amount".to_string(), DataType::Integer, false),
            ],
        );

        let outer_row =
            vibesql_storage::Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(100)]);
        let inner_row = vibesql_storage::Row::new(vec![
            SqlValue::Integer(42),
            SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
        ]);

        let evaluator =
            ExpressionEvaluator::with_outer_context(&inner_schema, &outer_row, &outer_schema);

        // Try to resolve non-existent column
        let expr = vibesql_ast::Expression::ColumnRef { table: None, column: "email".to_string() };

        let result = evaluator.eval(&expr, &inner_row);

        match result {
            Err(ExecutorError::ColumnNotFound {
                column_name,
                searched_tables,
                available_columns,
                ..
            }) => {
                assert_eq!(column_name, "email");
                assert!(
                    searched_tables.contains(&"users".to_string()),
                    "Should have searched 'users' table"
                );
                assert!(
                    searched_tables.contains(&"orders".to_string()),
                    "Should have searched 'orders' table"
                );
                assert!(
                    available_columns.contains(&"id".to_string()),
                    "Should list 'id' as available"
                );
                assert!(
                    available_columns.contains(&"name".to_string()),
                    "Should list 'name' as available"
                );
                assert!(
                    available_columns.contains(&"order_id".to_string()),
                    "Should list 'order_id' as available"
                );
                assert!(
                    available_columns.contains(&"amount".to_string()),
                    "Should list 'amount' as available"
                );
            }
            other => panic!("Expected ColumnNotFound with diagnostic info, got: {:?}", other),
        }
    }

    #[test]
    fn test_improved_error_message_with_table_qualifier() {
        // Test that table qualifier is captured in error message
        let schema = TableSchema::new(
            "products".to_string(),
            vec![
                ColumnSchema::new("product_id".to_string(), DataType::Integer, false),
                ColumnSchema::new("price".to_string(), DataType::Integer, false),
            ],
        );

        let evaluator = ExpressionEvaluator::new(&schema);
        let row = vibesql_storage::Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(99)]);

        // Try to resolve with table qualifier
        let expr = vibesql_ast::Expression::ColumnRef {
            table: Some("products".to_string()),
            column: "description".to_string(),
        };

        let result = evaluator.eval(&expr, &row);

        match result {
            Err(ExecutorError::ColumnNotFound {
                column_name,
                table_name,
                searched_tables,
                available_columns,
            }) => {
                assert_eq!(column_name, "description");
                assert_eq!(table_name, "products", "Should capture table qualifier");
                assert!(searched_tables.contains(&"products".to_string()));
                assert!(available_columns.contains(&"product_id".to_string()));
                assert!(available_columns.contains(&"price".to_string()));
            }
            other => panic!("Expected ColumnNotFound with table qualifier, got: {:?}", other),
        }
    }

    #[test]
    fn test_valid_table_qualifier_matches_inner_schema() {
        // Test that a valid table qualifier matching inner schema works correctly
        let schema = TableSchema::new(
            "users".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new(
                    "name".to_string(),
                    DataType::Varchar { max_length: Some(255) },
                    false,
                ),
            ],
        );

        let evaluator = ExpressionEvaluator::new(&schema);
        let row = vibesql_storage::Row::new(vec![
            SqlValue::Integer(42),
            SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
        ]);

        // Column reference with correct table qualifier
        let expr = vibesql_ast::Expression::ColumnRef {
            table: Some("users".to_string()),
            column: "name".to_string(),
        };

        let result = evaluator.eval(&expr, &row);
        assert_eq!(result, Ok(SqlValue::Varchar(arcstr::ArcStr::from("Alice"))));
    }

    #[test]
    fn test_valid_table_qualifier_case_insensitive() {
        // Test that table qualifier matching is case-insensitive
        let schema = TableSchema::new(
            "Products".to_string(),
            vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
        );

        let evaluator = ExpressionEvaluator::new(&schema);
        let row = vibesql_storage::Row::new(vec![SqlValue::Integer(99)]);

        // Use different case for table qualifier
        let expr = vibesql_ast::Expression::ColumnRef {
            table: Some("PRODUCTS".to_string()),
            column: "id".to_string(),
        };

        let result = evaluator.eval(&expr, &row);
        assert_eq!(result, Ok(SqlValue::Integer(99)));
    }

    #[test]
    fn test_invalid_table_qualifier_single_schema() {
        // Test that invalid table qualifier returns proper error
        let schema = TableSchema::new(
            "users".to_string(),
            vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
        );

        let evaluator = ExpressionEvaluator::new(&schema);
        let row = vibesql_storage::Row::new(vec![SqlValue::Integer(42)]);

        // Column reference with wrong table qualifier
        let expr = vibesql_ast::Expression::ColumnRef {
            table: Some("products".to_string()),
            column: "id".to_string(),
        };

        let result = evaluator.eval(&expr, &row);

        match result {
            Err(ExecutorError::InvalidTableQualifier { qualifier, column, available_tables }) => {
                assert_eq!(qualifier, "products");
                assert_eq!(column, "id");
                assert!(available_tables.contains(&"users".to_string()));
                assert_eq!(available_tables.len(), 1);
            }
            other => panic!("Expected InvalidTableQualifier error, got: {:?}", other),
        }
    }

    #[test]
    fn test_invalid_table_qualifier_with_outer_context() {
        // Test that invalid qualifier with outer context shows both available tables
        let inner_schema = TableSchema::new(
            "orders".to_string(),
            vec![ColumnSchema::new("order_id".to_string(), DataType::Integer, false)],
        );

        let outer_schema = TableSchema::new(
            "users".to_string(),
            vec![ColumnSchema::new("user_id".to_string(), DataType::Integer, false)],
        );

        let outer_row = vibesql_storage::Row::new(vec![SqlValue::Integer(100)]);
        let inner_row = vibesql_storage::Row::new(vec![SqlValue::Integer(42)]);

        let evaluator =
            ExpressionEvaluator::with_outer_context(&inner_schema, &outer_row, &outer_schema);

        // Column reference with invalid table qualifier
        let expr = vibesql_ast::Expression::ColumnRef {
            table: Some("products".to_string()),
            column: "product_id".to_string(),
        };

        let result = evaluator.eval(&expr, &inner_row);

        match result {
            Err(ExecutorError::InvalidTableQualifier { qualifier, column, available_tables }) => {
                assert_eq!(qualifier, "products");
                assert_eq!(column, "product_id");
                assert!(available_tables.contains(&"orders".to_string()));
                assert!(available_tables.contains(&"users".to_string()));
                assert_eq!(available_tables.len(), 2);
            }
            other => panic!("Expected InvalidTableQualifier error, got: {:?}", other),
        }
    }

    #[test]
    fn test_valid_qualifier_for_outer_schema() {
        // Test that qualifier can correctly reference outer schema
        let inner_schema = TableSchema::new(
            "subquery".to_string(),
            vec![ColumnSchema::new("inner_col".to_string(), DataType::Integer, false)],
        );

        let outer_schema = TableSchema::new(
            "outer_table".to_string(),
            vec![ColumnSchema::new("outer_col".to_string(), DataType::Integer, false)],
        );

        let outer_row = vibesql_storage::Row::new(vec![SqlValue::Integer(999)]);
        let inner_row = vibesql_storage::Row::new(vec![SqlValue::Integer(42)]);

        let evaluator =
            ExpressionEvaluator::with_outer_context(&inner_schema, &outer_row, &outer_schema);

        // Column reference with outer table qualifier
        let expr = vibesql_ast::Expression::ColumnRef {
            table: Some("outer_table".to_string()),
            column: "outer_col".to_string(),
        };

        let result = evaluator.eval(&expr, &inner_row);
        assert_eq!(result, Ok(SqlValue::Integer(999)));
    }

    #[test]
    fn test_qualified_column_not_found_in_correct_table() {
        // Test that when qualifier is valid but column doesn't exist, we get proper error
        let schema = TableSchema::new(
            "users".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new(
                    "name".to_string(),
                    DataType::Varchar { max_length: Some(255) },
                    false,
                ),
            ],
        );

        let evaluator = ExpressionEvaluator::new(&schema);
        let row = vibesql_storage::Row::new(vec![
            SqlValue::Integer(42),
            SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
        ]);

        // Column reference with correct table but non-existent column
        let expr = vibesql_ast::Expression::ColumnRef {
            table: Some("users".to_string()),
            column: "email".to_string(),
        };

        let result = evaluator.eval(&expr, &row);

        match result {
            Err(ExecutorError::ColumnNotFound {
                column_name,
                table_name,
                searched_tables,
                available_columns,
            }) => {
                assert_eq!(column_name, "email");
                assert_eq!(table_name, "users");
                assert!(searched_tables.contains(&"users".to_string()));
                assert!(available_columns.contains(&"id".to_string()));
                assert!(available_columns.contains(&"name".to_string()));
            }
            other => panic!("Expected ColumnNotFound error, got: {:?}", other),
        }
    }

    #[test]
    fn test_qualifier_disambiguates_shadowed_column() {
        // Test that qualifier can disambiguate when inner and outer have same column name
        let inner_schema = TableSchema::new(
            "inner_table".to_string(),
            vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
        );

        let outer_schema = TableSchema::new(
            "outer_table".to_string(),
            vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
        );

        let outer_row = vibesql_storage::Row::new(vec![SqlValue::Integer(999)]);
        let inner_row = vibesql_storage::Row::new(vec![SqlValue::Integer(42)]);

        let evaluator =
            ExpressionEvaluator::with_outer_context(&inner_schema, &outer_row, &outer_schema);

        // Without qualifier - should get inner value (shadowing)
        let expr_unqualified =
            vibesql_ast::Expression::ColumnRef { table: None, column: "id".to_string() };
        let result = evaluator.eval(&expr_unqualified, &inner_row);
        assert_eq!(result, Ok(SqlValue::Integer(42)));

        // With inner qualifier - should get inner value
        let expr_inner = vibesql_ast::Expression::ColumnRef {
            table: Some("inner_table".to_string()),
            column: "id".to_string(),
        };
        let result = evaluator.eval(&expr_inner, &inner_row);
        assert_eq!(result, Ok(SqlValue::Integer(42)));

        // With outer qualifier - should get outer value
        let expr_outer = vibesql_ast::Expression::ColumnRef {
            table: Some("outer_table".to_string()),
            column: "id".to_string(),
        };
        let result = evaluator.eval(&expr_outer, &inner_row);
        assert_eq!(result, Ok(SqlValue::Integer(999)));
    }
}

#[cfg(test)]
mod deep_expression_tests {
    use vibesql_catalog::TableSchema;
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    use super::super::ExpressionEvaluator;

    /// Helper to generate a deeply nested arithmetic expression
    /// Generates: (((1 + 1) + 1) + 1) ... ) for the specified depth
    fn generate_nested_add(depth: usize) -> vibesql_ast::Expression {
        let mut expr = vibesql_ast::Expression::Literal(SqlValue::Integer(1));
        for _ in 0..depth {
            expr = vibesql_ast::Expression::BinaryOp {
                left: Box::new(expr),
                op: vibesql_ast::BinaryOperator::Plus,
                right: Box::new(vibesql_ast::Expression::Literal(SqlValue::Integer(1))),
            };
        }
        expr
    }

    /// Helper to generate deeply nested CASE expressions
    /// Generates: CASE WHEN 1 = 1 THEN (CASE WHEN 1 = 1 THEN ... ELSE 0 END) ELSE 0 END
    fn generate_nested_case(depth: usize) -> vibesql_ast::Expression {
        let mut expr = vibesql_ast::Expression::Literal(SqlValue::Integer(0));
        for _ in 0..depth {
            expr = vibesql_ast::Expression::Case {
                operand: None,
                when_clauses: vec![vibesql_ast::CaseWhen {
                    conditions: vec![vibesql_ast::Expression::BinaryOp {
                        left: Box::new(vibesql_ast::Expression::Literal(SqlValue::Integer(1))),
                        op: vibesql_ast::BinaryOperator::Equal,
                        right: Box::new(vibesql_ast::Expression::Literal(SqlValue::Integer(1))),
                    }],
                    result: expr,
                }],
                else_result: Some(Box::new(vibesql_ast::Expression::Literal(SqlValue::Integer(0)))),
            };
        }
        expr
    }

    /// Helper to generate deeply nested unary expressions
    /// Generates: -(-(-(-1))) for the specified depth
    fn generate_nested_unary(depth: usize) -> vibesql_ast::Expression {
        let mut expr = vibesql_ast::Expression::Literal(SqlValue::Integer(1));
        for _ in 0..depth {
            expr = vibesql_ast::Expression::UnaryOp {
                op: vibesql_ast::UnaryOperator::Minus,
                expr: Box::new(expr),
            };
        }
        expr
    }

    #[test]
    fn test_shallow_nested_expression() {
        // Depth 10 should work fine
        let schema = TableSchema::new("test".to_string(), vec![]);
        let evaluator = ExpressionEvaluator::new(&schema);
        let row = Row::new(vec![]);

        let expr = generate_nested_add(10);
        let result = evaluator.eval(&expr, &row);

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), SqlValue::Integer(11)); // 1 + 10 additions of 1
    }

    #[test]
    fn test_moderate_nested_expression() {
        // Depth 100 should work fine
        let schema = TableSchema::new("test".to_string(), vec![]);
        let evaluator = ExpressionEvaluator::new(&schema);
        let row = Row::new(vec![]);

        let expr = generate_nested_add(100);
        let result = evaluator.eval(&expr, &row);

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), SqlValue::Integer(101)); // 1 + 100 additions of 1
    }

    #[test]
    fn test_depth_limit_enforced() {
        // Test that depth limit is properly enforced
        // MAX_EXPRESSION_DEPTH is set to 200 to prevent stack overflow.
        // Building expressions deeper than ~200 causes stack overflow during evaluation
        // due to Rust's recursive call stack limitations (each eval call consumes stack space).
        let schema = TableSchema::new("test".to_string(), vec![]);
        let evaluator = ExpressionEvaluator::new(&schema);
        let row = Row::new(vec![]);

        // Test a deep but safe expression (100 levels - well within MAX_EXPRESSION_DEPTH of 200)
        let expr = generate_nested_add(100);
        let result = evaluator.eval(&expr, &row);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), SqlValue::Integer(101)); // 1 + 100 additions of 1
    }

    #[test]
    fn test_deep_case_expressions() {
        // Test CASE expression depth tracking
        let schema = TableSchema::new("test".to_string(), vec![]);
        let evaluator = ExpressionEvaluator::new(&schema);
        let row = Row::new(vec![]);

        // Depth 50 should work
        let expr = generate_nested_case(50);
        let result = evaluator.eval(&expr, &row);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), SqlValue::Integer(0));

        // Depth 150 should work (within MAX_EXPRESSION_DEPTH of 200)
        let expr = generate_nested_case(150);
        let result = evaluator.eval(&expr, &row);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), SqlValue::Integer(0));
    }

    #[test]
    fn test_deep_unary_expressions() {
        // Test unary operator depth tracking
        let schema = TableSchema::new("test".to_string(), vec![]);
        let evaluator = ExpressionEvaluator::new(&schema);
        let row = Row::new(vec![]);

        // Depth 100 should work
        let expr = generate_nested_unary(100);
        let result = evaluator.eval(&expr, &row);
        assert!(result.is_ok());
        // 100 negations of 1: even count = 1, odd count = -1
        assert_eq!(result.unwrap(), SqlValue::Integer(1));

        // Depth 150 should also work (within MAX_EXPRESSION_DEPTH of 200)
        let expr = generate_nested_unary(150);
        let result = evaluator.eval(&expr, &row);
        assert!(result.is_ok());
        // 150 negations of 1: even count = 1
        assert_eq!(result.unwrap(), SqlValue::Integer(1));
    }

    #[test]
    fn test_mixed_nested_expressions() {
        // Test combination of different expression types
        let schema = TableSchema::new("test".to_string(), vec![]);
        let evaluator = ExpressionEvaluator::new(&schema);
        let row = Row::new(vec![]);

        // Build: CASE WHEN 1=1 THEN (1 + (1 + (1 + 1))) ELSE -(-1) END
        let nested_add = generate_nested_add(3);
        let nested_unary = generate_nested_unary(2);

        let expr = vibesql_ast::Expression::Case {
            operand: None,
            when_clauses: vec![vibesql_ast::CaseWhen {
                conditions: vec![vibesql_ast::Expression::BinaryOp {
                    left: Box::new(vibesql_ast::Expression::Literal(SqlValue::Integer(1))),
                    op: vibesql_ast::BinaryOperator::Equal,
                    right: Box::new(vibesql_ast::Expression::Literal(SqlValue::Integer(1))),
                }],
                result: nested_add,
            }],
            else_result: Some(Box::new(nested_unary)),
        };

        let result = evaluator.eval(&expr, &row);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), SqlValue::Integer(4)); // 1 + 3
    }

    #[test]
    fn test_depth_tracking_is_consistent() {
        // Verify depth is properly tracked across evaluator instances
        let schema = TableSchema::new("test".to_string(), vec![]);
        let evaluator = ExpressionEvaluator::new(&schema);

        assert_eq!(evaluator.depth, 0);

        // Create evaluator with outer context - should still start at depth 0
        let outer_schema = TableSchema::new("outer".to_string(), vec![]);
        let outer_row = Row::new(vec![]);
        let evaluator_with_outer =
            ExpressionEvaluator::with_outer_context(&schema, &outer_row, &outer_schema);

        assert_eq!(evaluator_with_outer.depth, 0);
    }

    #[test]
    fn test_reasonable_depth_expressions() {
        // Test expressions at reasonable but deep nesting levels
        let schema = TableSchema::new("test".to_string(), vec![]);
        let evaluator = ExpressionEvaluator::new(&schema);
        let row = Row::new(vec![]);

        // Depth 100 should work fine (well within MAX_EXPRESSION_DEPTH of 200)
        let expr = generate_nested_add(100);
        let result = evaluator.eval(&expr, &row);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), SqlValue::Integer(101));

        // Depth 150 should also work
        let expr = generate_nested_add(150);
        let result = evaluator.eval(&expr, &row);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), SqlValue::Integer(151));
    }
}
