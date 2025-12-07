//! Phase C proof-of-concept tests demonstrating end-to-end iterator pipelines

use super::*;

/// Phase C Proof-of-Concept: End-to-End Iterator Pipeline
///
/// This test demonstrates how iterators would be used for a complete query:
/// SELECT * FROM users WHERE age > 18 LIMIT 10
///
/// This validates that:
/// 1. TableScanIterator provides the data source
/// 2. FilterIterator applies WHERE conditions
/// 3. LIMIT works via .take()
/// 4. Everything composes naturally
#[test]
fn test_phase_c_proof_of_concept_full_pipeline() {
    // Simulated table: users(id, name, age)
    let schema = vibesql_catalog::TableSchema::new(
        "users".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "name".to_string(),
                vibesql_types::DataType::Varchar { max_length: None },
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "age".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
        ],
    );
    let from_schema = CombinedSchema::from_table("users".to_string(), schema);

    // Test data: 5 users with varying ages
    let from_rows = vec![
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
            SqlValue::Integer(25),
        ]),
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar(arcstr::ArcStr::from("Bob")),
            SqlValue::Integer(17),
        ]),
        Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar(arcstr::ArcStr::from("Charlie")),
            SqlValue::Integer(30),
        ]),
        Row::new(vec![
            SqlValue::Integer(4),
            SqlValue::Varchar(arcstr::ArcStr::from("Diana")),
            SqlValue::Integer(16),
        ]),
        Row::new(vec![
            SqlValue::Integer(5),
            SqlValue::Varchar(arcstr::ArcStr::from("Eve")),
            SqlValue::Integer(22),
        ]),
    ];

    // Stage 1: FROM - Create table scan iterator
    let scan = TableScanIterator::new(from_schema.clone(), from_rows);

    // Stage 2: WHERE age > 18
    let where_expr = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::ColumnRef {
            table: Some("users".to_string()),
            column: "age".to_string(),
        }),
        op: vibesql_ast::BinaryOperator::GreaterThan,
        right: Box::new(vibesql_ast::Expression::Literal(SqlValue::Integer(18))),
    };

    let evaluator = CombinedExpressionEvaluator::new(&from_schema);
    let filter = FilterIterator::new(scan, where_expr, evaluator);

    // Stage 3: LIMIT 10
    let limited = filter.take(10);

    // Stage 4: Execute (collect results)
    let results: Vec<_> = limited.collect::<Result<Vec<_>, _>>().unwrap();

    // Verify results: Should get Alice (25), Charlie (30), Eve (22)
    // Bob (17) and Diana (16) are filtered out
    assert_eq!(results.len(), 3);

    assert_eq!(results[0].values[0], SqlValue::Integer(1)); // Alice
    assert_eq!(results[0].values[2], SqlValue::Integer(25));

    assert_eq!(results[1].values[0], SqlValue::Integer(3)); // Charlie
    assert_eq!(results[1].values[2], SqlValue::Integer(30));

    assert_eq!(results[2].values[0], SqlValue::Integer(5)); // Eve
    assert_eq!(results[2].values[2], SqlValue::Integer(22));
}

/// Phase C Proof-of-Concept: Iterator Pipeline with JOIN
///
/// This test demonstrates iterator-based execution for a query with JOIN:
/// SELECT * FROM orders JOIN customers ON orders.customer_id = customers.id
/// WHERE orders.amount > 100 LIMIT 5
///
/// This validates that:
/// 1. LazyNestedLoopJoin streams through left side (orders)
/// 2. JOIN condition is evaluated correctly
/// 3. Early termination works (only processes enough to get 5 results)
#[test]
fn test_phase_c_proof_of_concept_join_pipeline() {
    // Setup schemas
    let orders_schema = vibesql_catalog::TableSchema::new(
        "orders".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "customer_id".to_string(),
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
    let customers_schema = vibesql_catalog::TableSchema::new(
        "customers".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "name".to_string(),
                vibesql_types::DataType::Varchar { max_length: None },
                false,
            ),
        ],
    );

    let orders_combined = CombinedSchema::from_table("orders".to_string(), orders_schema);
    let customers_combined = CombinedSchema::from_table("customers".to_string(), customers_schema);

    // Test data: 10 orders
    let orders_rows: Vec<_> = (1..=10)
        .map(|i| {
            Row::new(vec![
                SqlValue::Integer(i),
                SqlValue::Integer((i % 3) + 1), // customer_id cycles 1, 2, 3
                SqlValue::Integer(i * 50),      // amount: 50, 100, 150, ...
            ])
        })
        .collect();

    // 3 customers
    let customers_rows = vec![
        Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))]),
        Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("Bob"))]),
        Row::new(vec![SqlValue::Integer(3), SqlValue::Varchar(arcstr::ArcStr::from("Charlie"))]),
    ];

    // Stage 1: FROM orders (scan)
    let orders_scan = TableScanIterator::new(orders_combined.clone(), orders_rows);

    // Stage 2: JOIN customers ON orders.customer_id = customers.id
    let join_condition = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::ColumnRef {
            table: Some("orders".to_string()),
            column: "customer_id".to_string(),
        }),
        op: vibesql_ast::BinaryOperator::Equal,
        right: Box::new(vibesql_ast::Expression::ColumnRef {
            table: Some("customers".to_string()),
            column: "id".to_string(),
        }),
    };

    let join = LazyNestedLoopJoin::new(
        orders_scan,
        customers_combined.clone(),
        customers_rows,
        vibesql_ast::JoinType::Inner,
        Some(join_condition),
        None, // No database for test
    );

    // Stage 3: WHERE orders.amount > 100
    // Build combined schema for WHERE evaluation
    let mut combined_tables = orders_combined.table_schemas.clone();
    for (name, (start_idx, schema)) in customers_combined.table_schemas.iter() {
        combined_tables
            .insert(name.clone(), (orders_combined.total_columns + start_idx, schema.clone()));
    }
    let combined_schema = CombinedSchema {
        table_schemas: combined_tables,
        total_columns: orders_combined.total_columns + customers_combined.total_columns,
    };

    let where_expr = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::ColumnRef {
            table: Some("orders".to_string()),
            column: "amount".to_string(),
        }),
        op: vibesql_ast::BinaryOperator::GreaterThan,
        right: Box::new(vibesql_ast::Expression::Literal(SqlValue::Integer(100))),
    };

    let evaluator = CombinedExpressionEvaluator::new(&combined_schema);
    let filter = FilterIterator::new(join, where_expr, evaluator);

    // Stage 4: LIMIT 5
    let limited = filter.take(5);

    // Stage 5: Execute
    let results: Vec<_> = limited.collect::<Result<Vec<_>, _>>().unwrap();

    // Verify: Should get orders with amount > 100 (orders 3, 4, 5, 6, 7...)
    assert_eq!(results.len(), 5);

    // Each result should have 5 columns: orders(id, customer_id, amount) + customers(id, name)
    assert_eq!(results[0].values.len(), 5);

    // First result should be order 3 (amount 150) joined with customer
    assert_eq!(results[0].values[0], SqlValue::Integer(3)); // order.id
    assert_eq!(results[0].values[2], SqlValue::Integer(150)); // order.amount
}
