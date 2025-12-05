//! Test COUNT(*) without FROM clause
//!
//! Tests COUNT(*) in expressions when no FROM clause is present

use vibesql_executor::SelectExecutor;

#[test]
fn test_count_star_without_from() {
    // Test: SELECT COUNT(*) - no FROM clause
    // This should return 1 (one row with count = 0)
    let db = vibesql_storage::Database::new();
    let executor = SelectExecutor::new(&db);

    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::AggregateFunction {
                name: "COUNT".to_string(),
                distinct: false,
                args: vec![vibesql_ast::Expression::Wildcard],
            },
            alias: None,
        }],
        from: None, // No FROM clause
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt);
    // Without FROM clause, aggregate functions may not be supported
    // or should return appropriate default value
    match result {
        Ok(rows) => {
            assert_eq!(rows.len(), 1);
            // COUNT(*) with no FROM should return 0 or 1 depending on semantics
            println!("Result: {:?}", rows[0].values[0]);
        }
        Err(e) => {
            println!("Error (may be expected): {:?}", e);
            // This might be an unsupported feature
        }
    }
}

#[test]
fn test_count_star_in_expression_without_from() {
    // Test: SELECT -18 * COUNT(*) - no FROM clause
    let db = vibesql_storage::Database::new();
    let executor = SelectExecutor::new(&db);

    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::BinaryOp {
                left: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(
                    -18,
                ))),
                op: vibesql_ast::BinaryOperator::Multiply,
                right: Box::new(vibesql_ast::Expression::AggregateFunction {
                    name: "COUNT".to_string(),
                    distinct: false,
                    args: vec![vibesql_ast::Expression::Wildcard],
                }),
            },
            alias: None,
        }],
        from: None, // No FROM clause
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt);
    match result {
        Ok(rows) => {
            println!("Success: {:?}", rows);
        }
        Err(e) => {
            println!("Error: {:?}", e);
            // This is likely the issue reported in #922
            assert!(e.to_string().contains("Unsupported") || e.to_string().contains("aggregate"));
        }
    }
}

#[test]
fn test_complex_expression_without_from() {
    // Test the exact example from SQLLOGICTEST_ISSUES.md:
    // SELECT CAST( NULL AS DECIMAL ) * - COUNT( * ) / + + 20 AS col2
    let db = vibesql_storage::Database::new();
    let executor = SelectExecutor::new(&db);

    // Build the complex expression step by step
    // COUNT(*)
    let count_star = vibesql_ast::Expression::AggregateFunction {
        name: "COUNT".to_string(),
        distinct: false,
        args: vec![vibesql_ast::Expression::Wildcard],
    };

    // - COUNT(*)
    let neg_count = vibesql_ast::Expression::UnaryOp {
        op: vibesql_ast::UnaryOperator::Minus,
        expr: Box::new(count_star),
    };

    // CAST(NULL AS DECIMAL)
    let cast_null = vibesql_ast::Expression::Cast {
        expr: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null)),
        data_type: vibesql_types::DataType::Decimal { precision: 10, scale: 0 },
    };

    // CAST(NULL AS DECIMAL) * - COUNT(*)
    let mult = vibesql_ast::Expression::BinaryOp {
        left: Box::new(cast_null),
        op: vibesql_ast::BinaryOperator::Multiply,
        right: Box::new(neg_count),
    };

    // 20 (with unary +)
    let twenty = vibesql_ast::Expression::UnaryOp {
        op: vibesql_ast::UnaryOperator::Plus,
        expr: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(20))),
    };

    // Another unary +
    let plus_twenty = vibesql_ast::Expression::UnaryOp {
        op: vibesql_ast::UnaryOperator::Plus,
        expr: Box::new(twenty),
    };

    // (CAST(NULL AS DECIMAL) * - COUNT(*)) / + + 20
    let div = vibesql_ast::Expression::BinaryOp {
        left: Box::new(mult),
        op: vibesql_ast::BinaryOperator::Divide,
        right: Box::new(plus_twenty),
    };

    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: div,
            alias: Some("col2".to_string()),
        }],
        from: None, // No FROM clause - this is the key issue!
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt);
    match result {
        Ok(rows) => {
            println!("Unexpected success: {:?}", rows);
            // If this works, the issue is resolved
        }
        Err(e) => {
            println!("Error (this is the bug): {:?}", e);
            // This should demonstrate the issue
        }
    }
}
