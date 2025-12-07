//! Join iterator tests

use super::*;

#[test]
fn test_lazy_nested_loop_join_cross() {
    let (left_schema, right_schema) = test_join_schemas();

    let left_rows = vec![
        Row::from_vec(vec![SqlValue::Integer(1), SqlValue::Integer(10)]),
        Row::from_vec(vec![SqlValue::Integer(2), SqlValue::Integer(20)]),
    ];
    let right_rows = vec![
        Row::from_vec(vec![SqlValue::Integer(1), SqlValue::Integer(100)]),
        Row::from_vec(vec![SqlValue::Integer(2), SqlValue::Integer(200)]),
    ];

    let left_iter = TableScanIterator::new(left_schema.clone(), left_rows);

    let mut join = LazyNestedLoopJoin::new(
        left_iter,
        right_schema,
        right_rows,
        vibesql_ast::JoinType::Cross,
        None, // No condition for CROSS JOIN
        None, // No database for test
    );

    // CROSS JOIN: 2 left x 2 right = 4 rows
    let results: Vec<_> = join.by_ref().collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(results.len(), 4);

    // Check first row: (1, 10, 1, 100)
    assert_eq!(
        results[0].values.to_vec(),
        vec![
            SqlValue::Integer(1),
            SqlValue::Integer(10),
            SqlValue::Integer(1),
            SqlValue::Integer(100)
        ]
    );

    // Check last row: (2, 20, 2, 200)
    assert_eq!(
        results[3].values.to_vec(),
        vec![
            SqlValue::Integer(2),
            SqlValue::Integer(20),
            SqlValue::Integer(2),
            SqlValue::Integer(200)
        ]
    );
}

#[test]
fn test_lazy_nested_loop_join_inner_with_condition() {
    let (left_schema, right_schema) = test_join_schemas();

    let left_rows = vec![
        Row::from_vec(vec![SqlValue::Integer(1), SqlValue::Integer(10)]),
        Row::from_vec(vec![SqlValue::Integer(2), SqlValue::Integer(20)]),
        Row::from_vec(vec![SqlValue::Integer(3), SqlValue::Integer(30)]),
    ];
    let right_rows = vec![
        Row::from_vec(vec![SqlValue::Integer(1), SqlValue::Integer(100)]),
        Row::from_vec(vec![SqlValue::Integer(2), SqlValue::Integer(200)]),
    ];

    let left_iter = TableScanIterator::new(left_schema.clone(), left_rows);

    // Condition: t1.id = t2.id (column 0 = column 2)
    let condition = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::ColumnRef {
            table: Some("t1".to_string()),
            column: "id".to_string(),
        }),
        op: vibesql_ast::BinaryOperator::Equal,
        right: Box::new(vibesql_ast::Expression::ColumnRef {
            table: Some("t2".to_string()),
            column: "id".to_string(),
        }),
    };

    let mut join = LazyNestedLoopJoin::new(
        left_iter,
        right_schema,
        right_rows,
        vibesql_ast::JoinType::Inner,
        Some(condition),
        None, // No database for test
    );

    // INNER JOIN with condition: only (1,1) and (2,2) match
    let results: Vec<_> = join.by_ref().collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(results.len(), 2);

    // (1, 10, 1, 100)
    assert_eq!(
        results[0].values.to_vec(),
        vec![
            SqlValue::Integer(1),
            SqlValue::Integer(10),
            SqlValue::Integer(1),
            SqlValue::Integer(100)
        ]
    );

    // (2, 20, 2, 200)
    assert_eq!(
        results[1].values.to_vec(),
        vec![
            SqlValue::Integer(2),
            SqlValue::Integer(20),
            SqlValue::Integer(2),
            SqlValue::Integer(200)
        ]
    );
}

#[test]
fn test_lazy_nested_loop_join_left_outer() {
    let (left_schema, right_schema) = test_join_schemas();

    let left_rows = vec![
        Row::from_vec(vec![SqlValue::Integer(1), SqlValue::Integer(10)]),
        Row::from_vec(vec![SqlValue::Integer(3), SqlValue::Integer(30)]), // No match in right
    ];
    let right_rows = vec![Row::from_vec(vec![SqlValue::Integer(1), SqlValue::Integer(100)])];

    let left_iter = TableScanIterator::new(left_schema.clone(), left_rows);

    // Condition: t1.id = t2.id
    let condition = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::ColumnRef {
            table: Some("t1".to_string()),
            column: "id".to_string(),
        }),
        op: vibesql_ast::BinaryOperator::Equal,
        right: Box::new(vibesql_ast::Expression::ColumnRef {
            table: Some("t2".to_string()),
            column: "id".to_string(),
        }),
    };

    let mut join = LazyNestedLoopJoin::new(
        left_iter,
        right_schema,
        right_rows,
        vibesql_ast::JoinType::LeftOuter,
        Some(condition),
        None, // No database for test
    );

    let results: Vec<_> = join.by_ref().collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(results.len(), 2);

    // First row: (1, 10, 1, 100) - match
    assert_eq!(
        results[0].values.to_vec(),
        vec![
            SqlValue::Integer(1),
            SqlValue::Integer(10),
            SqlValue::Integer(1),
            SqlValue::Integer(100)
        ]
    );

    // Second row: (3, 30, NULL, NULL) - no match, left with NULLs
    assert_eq!(
        results[1].values.to_vec(),
        vec![SqlValue::Integer(3), SqlValue::Integer(30), SqlValue::Null, SqlValue::Null]
    );
}

#[test]
fn test_lazy_nested_loop_join_early_termination() {
    let (left_schema, right_schema) = test_join_schemas();

    // Large left side
    let left_rows: Vec<_> = (1..=1000)
        .map(|i| Row::from_vec(vec![SqlValue::Integer(i), SqlValue::Integer(i * 10)]))
        .collect();

    // Small right side
    let right_rows: Vec<_> = (1..=10)
        .map(|i| Row::from_vec(vec![SqlValue::Integer(i), SqlValue::Integer(i * 100)]))
        .collect();

    let left_iter = TableScanIterator::new(left_schema.clone(), left_rows);

    let join = LazyNestedLoopJoin::new(
        left_iter,
        right_schema,
        right_rows,
        vibesql_ast::JoinType::Cross,
        None,
        None, // No database for test
    );

    // Only take first 5 rows - should not materialize all 1000 left rows
    let results: Vec<_> = join.take(5).collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(results.len(), 5);

    // Verify we got the expected rows (first left row with first 5 right rows)
    for result in &results {
        assert_eq!(result.values[0], SqlValue::Integer(1)); // Left id stays 1
        assert_eq!(result.values[1], SqlValue::Integer(10)); // Left value stays 10
    }
}
