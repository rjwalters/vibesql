//! Test for issue #990: COUNT(*) not evaluated in arithmetic expressions
//!
//! Reproduces the exact failing case from SQLLogicTest

use vibesql_executor::SelectExecutor;

#[test]
fn test_issue_990_multiple_unary_plus() {
    // From the issue: SELECT + + 5 + 92 * COUNT( * )
    // Expected: 97 (assuming 1 row: 5 + 92 * 1 = 97)
    // Actual: 5.000 (bug - COUNT(*) returns 0 or is ignored)

    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "test".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "id".to_string(),
            vibesql_types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema).unwrap();

    // Insert 1 row
    db.insert_row("test", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(1)]))
        .unwrap();

    let executor = SelectExecutor::new(&db);

    // Build the exact expression from the issue: + + 5 + 92 * COUNT(*)
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
            values: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::BinaryOp {
                left: Box::new(vibesql_ast::Expression::UnaryOp {
                    op: vibesql_ast::UnaryOperator::Plus,
                    expr: Box::new(vibesql_ast::Expression::UnaryOp {
                        op: vibesql_ast::UnaryOperator::Plus,
                        expr: Box::new(vibesql_ast::Expression::Literal(
                            vibesql_types::SqlValue::Integer(5),
                        )),
                    }),
                }),
                op: vibesql_ast::BinaryOperator::Plus,
                right: Box::new(vibesql_ast::Expression::BinaryOp {
                    left: Box::new(vibesql_ast::Expression::Literal(
                        vibesql_types::SqlValue::Integer(92),
                    )),
                    op: vibesql_ast::BinaryOperator::Multiply,
                    right: Box::new(vibesql_ast::Expression::AggregateFunction {
                        name: vibesql_ast::FunctionIdentifier::new("COUNT"),
                        distinct: false,
                        args: vec![vibesql_ast::Expression::Wildcard],
                        order_by: None,
                    }),
                }),
            },
            alias: None,
            source_text: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "test".to_string(),
            alias: None,
            column_aliases: None, quoted: false,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);

    // Expected: 97 (5 + 92 * 1)
    println!("Result: {:?}", result[0].values[0]);

    match &result[0].values[0] {
        vibesql_types::SqlValue::Integer(n) => assert_eq!(*n, 97, "Expected 97, got {}", n),
        vibesql_types::SqlValue::Numeric(n) => {
            // Check if it's close to 97 (allowing for floating point comparison)
            assert!((*n - 97.0).abs() < 0.001, "Expected 97, got {}", n);
        }
        other => panic!("Expected Integer or Numeric, got {:?}", other),
    }
}

#[test]
fn test_issue_990_simpler_case() {
    // Simpler case: SELECT 5 + 92 * COUNT(*) FROM test
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "test".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "id".to_string(),
            vibesql_types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema).unwrap();

    // Insert 1 row
    db.insert_row("test", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(1)]))
        .unwrap();

    let executor = SelectExecutor::new(&db);

    // Build: 5 + 92 * COUNT(*)
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
            values: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::BinaryOp {
                left: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(
                    5,
                ))),
                op: vibesql_ast::BinaryOperator::Plus,
                right: Box::new(vibesql_ast::Expression::BinaryOp {
                    left: Box::new(vibesql_ast::Expression::Literal(
                        vibesql_types::SqlValue::Integer(92),
                    )),
                    op: vibesql_ast::BinaryOperator::Multiply,
                    right: Box::new(vibesql_ast::Expression::AggregateFunction {
                        name: vibesql_ast::FunctionIdentifier::new("COUNT"),
                        distinct: false,
                        args: vec![vibesql_ast::Expression::Wildcard],
                        order_by: None,
                    }),
                }),
            },
            alias: None,
            source_text: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "test".to_string(),
            alias: None,
            column_aliases: None, quoted: false,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);

    println!("Simpler case result: {:?}", result[0].values[0]);

    // Expected: 97 (5 + 92 * 1)
    match &result[0].values[0] {
        vibesql_types::SqlValue::Integer(n) => assert_eq!(*n, 97, "Expected 97, got {}", n),
        vibesql_types::SqlValue::Numeric(n) => {
            assert!((*n - 97.0).abs() < 0.001, "Expected 97, got {}", n);
        }
        other => panic!("Expected Integer or Numeric, got {:?}", other),
    }
}
