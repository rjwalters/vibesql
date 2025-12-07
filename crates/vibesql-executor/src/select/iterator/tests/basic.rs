//! Basic iterator tests for scan, filter, and projection

use super::*;

#[test]
fn test_table_scan_iterator_empty() {
    let schema = test_schema();
    let rows = vec![];
    let mut iter = TableScanIterator::new(schema.clone(), rows);

    // Verify schema is accessible (len() is always >= 0, so just access it)
    let _ = iter.schema().table_schemas.len();
    assert_eq!(iter.next(), None);
    assert_eq!(iter.size_hint(), (0, Some(0)));
}

#[test]
fn test_table_scan_iterator_with_rows() {
    let schema = test_schema();
    let rows = vec![
        Row::from_vec(vec![SqlValue::Integer(1)]),
        Row::from_vec(vec![SqlValue::Integer(2)]),
        Row::from_vec(vec![SqlValue::Integer(3)]),
    ];
    let mut iter = TableScanIterator::new(schema.clone(), rows);

    // Verify schema is accessible (len() is always >= 0, so just access it)
    let _ = iter.schema().table_schemas.len();
    assert_eq!(iter.size_hint(), (3, Some(3)));

    assert_eq!(iter.next().unwrap().unwrap().values.to_vec(), vec![SqlValue::Integer(1)]);
    assert_eq!(iter.next().unwrap().unwrap().values.to_vec(), vec![SqlValue::Integer(2)]);
    assert_eq!(iter.next().unwrap().unwrap().values.to_vec(), vec![SqlValue::Integer(3)]);
    assert_eq!(iter.next(), None);
}

#[test]
fn test_filter_iterator_all_pass() {
    let schema = test_schema();
    let rows = vec![
        Row::from_vec(vec![SqlValue::Integer(1)]),
        Row::from_vec(vec![SqlValue::Integer(2)]),
        Row::from_vec(vec![SqlValue::Integer(3)]),
    ];
    let scan = TableScanIterator::new(schema.clone(), rows);

    // Predicate that always returns true
    let predicate = vibesql_ast::Expression::Literal(SqlValue::Boolean(true));
    let evaluator = CombinedExpressionEvaluator::new(&schema);
    let mut filter = FilterIterator::new(scan, predicate, evaluator);

    // Verify schema is accessible (len() is always >= 0, so just access it)
    let _ = filter.schema().table_schemas.len();
    assert_eq!(filter.next().unwrap().unwrap().values.to_vec(), vec![SqlValue::Integer(1)]);
    assert_eq!(filter.next().unwrap().unwrap().values.to_vec(), vec![SqlValue::Integer(2)]);
    assert_eq!(filter.next().unwrap().unwrap().values.to_vec(), vec![SqlValue::Integer(3)]);
    assert_eq!(filter.next(), None);
}

#[test]
fn test_filter_iterator_none_pass() {
    let schema = test_schema();
    let rows = vec![
        Row::from_vec(vec![SqlValue::Integer(1)]),
        Row::from_vec(vec![SqlValue::Integer(2)]),
        Row::from_vec(vec![SqlValue::Integer(3)]),
    ];
    let scan = TableScanIterator::new(schema.clone(), rows);

    // Predicate that always returns false
    let predicate = vibesql_ast::Expression::Literal(SqlValue::Boolean(false));
    let evaluator = CombinedExpressionEvaluator::new(&schema);
    let mut filter = FilterIterator::new(scan, predicate, evaluator);

    // Verify schema is accessible (len() is always >= 0, so just access it)
    let _ = filter.schema().table_schemas.len();
    assert_eq!(filter.next(), None);
}

#[test]
fn test_filter_iterator_null_is_falsy() {
    let schema = test_schema();
    let rows = vec![Row::from_vec(vec![SqlValue::Integer(1)])];
    let scan = TableScanIterator::new(schema.clone(), rows);

    // Predicate that returns NULL (should filter out)
    let predicate = vibesql_ast::Expression::Literal(SqlValue::Null);
    let evaluator = CombinedExpressionEvaluator::new(&schema);
    let mut filter = FilterIterator::new(scan, predicate, evaluator);

    assert_eq!(filter.next(), None);
}

#[test]
fn test_filter_iterator_integer_truthy() {
    let schema = test_schema();
    let rows = vec![Row::from_vec(vec![SqlValue::Integer(1)]), Row::from_vec(vec![SqlValue::Integer(2)])];
    let scan = TableScanIterator::new(schema.clone(), rows);

    // Predicate that returns non-zero integer (truthy)
    let predicate = vibesql_ast::Expression::Literal(SqlValue::Integer(42));
    let evaluator = CombinedExpressionEvaluator::new(&schema);
    let mut filter = FilterIterator::new(scan, predicate, evaluator);

    assert_eq!(filter.next().unwrap().unwrap().values.to_vec(), vec![SqlValue::Integer(1)]);
    assert_eq!(filter.next().unwrap().unwrap().values.to_vec(), vec![SqlValue::Integer(2)]);
    assert_eq!(filter.next(), None);
}

#[test]
fn test_filter_iterator_zero_is_falsy() {
    let schema = test_schema();
    let rows = vec![Row::from_vec(vec![SqlValue::Integer(1)])];
    let scan = TableScanIterator::new(schema.clone(), rows);

    // Predicate that returns 0 (falsy)
    let predicate = vibesql_ast::Expression::Literal(SqlValue::Integer(0));
    let evaluator = CombinedExpressionEvaluator::new(&schema);
    let mut filter = FilterIterator::new(scan, predicate, evaluator);

    assert_eq!(filter.next(), None);
}

#[test]
fn test_evaluator_direct() {
    // Direct test of evaluator with column reference
    let table_schema = vibesql_catalog::TableSchema::new(
        "test".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "age".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
        ],
    );
    let schema = CombinedSchema::from_table("test".to_string(), table_schema);
    let database = vibesql_storage::Database::new();
    let evaluator = CombinedExpressionEvaluator::with_database(&schema, &database);

    let predicate = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::ColumnRef {
            table: None,
            column: "age".to_string(),
        }),
        op: vibesql_ast::BinaryOperator::GreaterThan,
        right: Box::new(vibesql_ast::Expression::Literal(SqlValue::Integer(18))),
    };

    // Test row 1: age=25, should be true (25 > 18)
    let row1 = Row::from_vec(vec![SqlValue::Integer(1), SqlValue::Integer(25)]);
    let result1 = evaluator.eval(&predicate, &row1).unwrap();
    println!("Row 1 (age=25): {:?}", result1);

    // Test row 2: age=17, should be false (17 > 18 is false)
    let row2 = Row::from_vec(vec![SqlValue::Integer(2), SqlValue::Integer(17)]);
    let result2 = evaluator.eval(&predicate, &row2).unwrap();
    println!("Row 2 (age=17): {:?}", result2);

    // Test row 3: age=30, should be true (30 > 18)
    let row3 = Row::from_vec(vec![SqlValue::Integer(3), SqlValue::Integer(30)]);
    let result3 = evaluator.eval(&predicate, &row3).unwrap();
    println!("Row 3 (age=30): {:?}", result3);

    // Verify expected values
    assert!(matches!(result1, SqlValue::Boolean(true)), "Row 1 should be true, got {:?}", result1);
    assert!(
        matches!(result2, SqlValue::Boolean(false)),
        "Row 2 should be false, got {:?}",
        result2
    );
    assert!(matches!(result3, SqlValue::Boolean(true)), "Row 3 should be true, got {:?}", result3);
}

#[test]
fn test_filter_with_column_ref() {
    // Test filtering with column reference comparison
    let table_schema = vibesql_catalog::TableSchema::new(
        "test".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "age".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
        ],
    );
    let schema = CombinedSchema::from_table("test".to_string(), table_schema);

    let rows = vec![
        Row::from_vec(vec![SqlValue::Integer(1), SqlValue::Integer(25)]),
        Row::from_vec(vec![SqlValue::Integer(2), SqlValue::Integer(17)]),
        Row::from_vec(vec![SqlValue::Integer(3), SqlValue::Integer(30)]),
    ];
    let scan = TableScanIterator::new(schema.clone(), rows);

    // Predicate: age > 18 (using unqualified column reference)
    let predicate = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::ColumnRef {
            table: None, // Try without table qualifier
            column: "age".to_string(),
        }),
        op: vibesql_ast::BinaryOperator::GreaterThan,
        right: Box::new(vibesql_ast::Expression::Literal(SqlValue::Integer(18))),
    };

    let database = vibesql_storage::Database::new();
    let evaluator = CombinedExpressionEvaluator::with_database(&schema, &database);
    let filter = FilterIterator::new(scan, predicate, evaluator);

    // Collect ALL results to see what's happening
    let results: Vec<_> = filter.collect::<Result<Vec<_>, _>>().unwrap();

    // Should get 2 results: id 1 (age 25) and id 3 (age 30)
    // Row 2 (age 17) should be filtered out
    assert_eq!(
        results.len(),
        2,
        "Expected 2 results, got {} results: {:?}",
        results.len(),
        results
    );

    assert_eq!(results[0].values.to_vec(), vec![SqlValue::Integer(1), SqlValue::Integer(25)]);
    assert_eq!(results[1].values.to_vec(), vec![SqlValue::Integer(3), SqlValue::Integer(30)]);
}

#[test]
fn test_projection_iterator_identity() {
    let schema = test_schema();
    let rows = vec![Row::from_vec(vec![SqlValue::Integer(1)]), Row::from_vec(vec![SqlValue::Integer(2)])];
    let scan = TableScanIterator::new(schema.clone(), rows);

    // Identity projection (no-op)
    let project_fn = |row: Row| Ok(row);
    let mut project = ProjectionIterator::new(scan, schema.clone(), project_fn);

    // Verify schema is accessible (len() is always >= 0, so just access it)
    let _ = project.schema().table_schemas.len();
    assert_eq!(project.next().unwrap().unwrap().values.to_vec(), vec![SqlValue::Integer(1)]);
    assert_eq!(project.next().unwrap().unwrap().values.to_vec(), vec![SqlValue::Integer(2)]);
    assert_eq!(project.next(), None);
}

#[test]
fn test_projection_iterator_transform() {
    let schema = test_schema();
    let rows = vec![Row::from_vec(vec![SqlValue::Integer(1)]), Row::from_vec(vec![SqlValue::Integer(2)])];
    let scan = TableScanIterator::new(schema.clone(), rows);

    // Double each value
    let project_fn = |mut row: Row| {
        if let SqlValue::Integer(n) = row.values[0] {
            row.values[0] = SqlValue::Integer(n * 2);
        }
        Ok(row)
    };
    let mut project = ProjectionIterator::new(scan, schema.clone(), project_fn);

    assert_eq!(project.next().unwrap().unwrap().values.to_vec(), vec![SqlValue::Integer(2)]);
    assert_eq!(project.next().unwrap().unwrap().values.to_vec(), vec![SqlValue::Integer(4)]);
    assert_eq!(project.next(), None);
}

#[test]
fn test_chained_iterators() {
    let schema = test_schema();
    let rows = vec![
        Row::from_vec(vec![SqlValue::Integer(1)]),
        Row::from_vec(vec![SqlValue::Integer(2)]),
        Row::from_vec(vec![SqlValue::Integer(3)]),
        Row::from_vec(vec![SqlValue::Integer(4)]),
    ];
    let scan = TableScanIterator::new(schema.clone(), rows);

    // Filter for values > 1
    let predicate = vibesql_ast::Expression::Literal(SqlValue::Integer(1)); // Truthy, for demo
    let evaluator = CombinedExpressionEvaluator::new(&schema);
    let filter = FilterIterator::new(scan, predicate, evaluator);

    // Then double each value
    let project_fn = |mut row: Row| {
        if let SqlValue::Integer(n) = row.values[0] {
            row.values[0] = SqlValue::Integer(n * 2);
        }
        Ok(row)
    };
    let mut project = ProjectionIterator::new(filter, schema.clone(), project_fn);

    // Should get all rows (filter passes all), doubled
    assert_eq!(project.next().unwrap().unwrap().values.to_vec(), vec![SqlValue::Integer(2)]);
    assert_eq!(project.next().unwrap().unwrap().values.to_vec(), vec![SqlValue::Integer(4)]);
    assert_eq!(project.next().unwrap().unwrap().values.to_vec(), vec![SqlValue::Integer(6)]);
    assert_eq!(project.next().unwrap().unwrap().values.to_vec(), vec![SqlValue::Integer(8)]);
    assert_eq!(project.next(), None);
}

#[test]
fn test_iterator_take_limit() {
    let schema = test_schema();
    let rows = vec![
        Row::from_vec(vec![SqlValue::Integer(1)]),
        Row::from_vec(vec![SqlValue::Integer(2)]),
        Row::from_vec(vec![SqlValue::Integer(3)]),
        Row::from_vec(vec![SqlValue::Integer(4)]),
        Row::from_vec(vec![SqlValue::Integer(5)]),
    ];
    let scan = TableScanIterator::new(schema.clone(), rows);

    // Use standard iterator take() to implement LIMIT
    let results: Vec<_> = scan.take(2).collect();

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].as_ref().unwrap().values.to_vec(), vec![SqlValue::Integer(1)]);
    assert_eq!(results[1].as_ref().unwrap().values.to_vec(), vec![SqlValue::Integer(2)]);
}
