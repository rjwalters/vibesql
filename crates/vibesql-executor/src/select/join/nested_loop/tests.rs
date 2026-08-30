//! Tests for nested loop join implementations.

use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

use super::{sequential::execute_nested_loop_classic, *};
use crate::schema::CombinedSchema;

/// Create a CombinedSchema for testing
fn create_combined_schema(
    left_table: &str,
    left_cols: Vec<(&str, DataType)>,
    right_table: &str,
    right_cols: Vec<(&str, DataType)>,
) -> CombinedSchema {
    let left_schema = TableSchema::new(
        left_table.to_string(),
        left_cols
            .iter()
            .map(|(name, dtype)| ColumnSchema::new(name.to_string(), dtype.clone(), true))
            .collect(),
    );

    let right_schema = TableSchema::new(
        right_table.to_string(),
        right_cols
            .iter()
            .map(|(name, dtype)| ColumnSchema::new(name.to_string(), dtype.clone(), true))
            .collect(),
    );

    CombinedSchema::combine(
        CombinedSchema::from_table(left_table.to_string(), left_schema),
        right_table.to_string(),
        right_schema,
    )
}

// ===== Tests for execute_nested_loop_classic (sequential path) =====

#[test]
fn test_nested_loop_classic_simple_condition() {
    // Test basic nested loop join with a simple equality condition
    let left_rows: Vec<Row> = vec![
        Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))]),
        Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("Bob"))]),
        Row::new(vec![SqlValue::Integer(3), SqlValue::Varchar(arcstr::ArcStr::from("Charlie"))]),
    ];

    let right_rows: Vec<Row> = vec![
        Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(100)]),
        Row::new(vec![SqlValue::Integer(2), SqlValue::Integer(200)]),
        Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(150)]),
    ];

    let schema = create_combined_schema(
        "users",
        vec![("id", DataType::Integer), ("name", DataType::Varchar { max_length: Some(50) })],
        "orders",
        vec![("user_id", DataType::Integer), ("amount", DataType::Integer)],
    );

    let db = Database::default();
    let timeout_ctx = crate::timeout::TimeoutContext::new_default();

    // Condition: users.id = orders.user_id (column 0 = column 2 in combined row)
    let condition = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::ColumnRef(
            vibesql_ast::ColumnIdentifier::qualified("users", false, "id", false),
        )),
        op: vibesql_ast::BinaryOperator::Equal,
        right: Box::new(vibesql_ast::Expression::ColumnRef(
            vibesql_ast::ColumnIdentifier::qualified("orders", false, "user_id", false),
        )),
    };

    let result = execute_nested_loop_classic(
        &left_rows,
        &right_rows,
        &Some(condition),
        &schema,
        &db,
        &timeout_ctx,
        &["users".to_string()],
        &["orders".to_string()],
        None, // outer_row - not used in this test
        None, // outer_schema - not used in this test
    )
    .unwrap();

    // Should have 3 matches: Alice (1) x 2 orders, Bob (1) x 1 order
    assert_eq!(result.len(), 3);

    // Verify Alice appears twice (2 orders with user_id=1)
    let alice_count = result.iter().filter(|r| r.values[0] == SqlValue::Integer(1)).count();
    assert_eq!(alice_count, 2);

    // Verify Bob appears once
    let bob_count = result.iter().filter(|r| r.values[0] == SqlValue::Integer(2)).count();
    assert_eq!(bob_count, 1);

    // Verify Charlie doesn't appear (no orders)
    let charlie_count = result.iter().filter(|r| r.values[0] == SqlValue::Integer(3)).count();
    assert_eq!(charlie_count, 0);
}

#[test]
fn test_nested_loop_classic_no_condition() {
    // Test cross product (no condition)
    let left_rows: Vec<Row> =
        vec![Row::new(vec![SqlValue::Integer(1)]), Row::new(vec![SqlValue::Integer(2)])];

    let right_rows: Vec<Row> = vec![
        Row::new(vec![SqlValue::Integer(10)]),
        Row::new(vec![SqlValue::Integer(20)]),
        Row::new(vec![SqlValue::Integer(30)]),
    ];

    let schema = create_combined_schema(
        "left",
        vec![("a", DataType::Integer)],
        "right",
        vec![("b", DataType::Integer)],
    );

    let db = Database::default();
    let timeout_ctx = crate::timeout::TimeoutContext::new_default();

    let result = execute_nested_loop_classic(
        &left_rows,
        &right_rows,
        &None,
        &schema,
        &db,
        &timeout_ctx,
        &["left".to_string()],
        &["right".to_string()],
        None, // outer_row - not used in this test
        None, // outer_schema - not used in this test
    )
    .unwrap();

    // Cross product: 2 x 3 = 6 rows
    assert_eq!(result.len(), 6);
}

#[test]
fn test_nested_loop_classic_empty_input() {
    let left_rows: Vec<Row> = vec![];
    let right_rows: Vec<Row> = vec![Row::new(vec![SqlValue::Integer(1)])];

    let schema = create_combined_schema(
        "left",
        vec![("a", DataType::Integer)],
        "right",
        vec![("b", DataType::Integer)],
    );

    let db = Database::default();
    let timeout_ctx = crate::timeout::TimeoutContext::new_default();

    let result = execute_nested_loop_classic(
        &left_rows,
        &right_rows,
        &None,
        &schema,
        &db,
        &timeout_ctx,
        &["left".to_string()],
        &["right".to_string()],
        None, // outer_row - not used in this test
        None, // outer_schema - not used in this test
    )
    .unwrap();

    assert_eq!(result.len(), 0);
}

// ===== Tests for parallel nested loop join (when feature = "parallel") =====

#[cfg(feature = "parallel")]
mod parallel_tests {
    use super::*;
    use crate::evaluator::CombinedExpressionEvaluator;

    #[test]
    fn test_parallel_nested_loop_simple() {
        // Test parallel nested loop with a simple condition
        let left_rows: Vec<Row> = (0..100)
            .map(|i| {
                Row::new(vec![
                    SqlValue::Integer(i % 10), // id (0-9, repeating)
                    SqlValue::Varchar(arcstr::ArcStr::from(format!("left{}", i))),
                ])
            })
            .collect();

        let right_rows: Vec<Row> = (0..50)
            .map(|i| {
                Row::new(vec![
                    SqlValue::Integer(i % 10), // user_id (0-9, repeating)
                    SqlValue::Integer(i * 100),
                ])
            })
            .collect();

        let schema = create_combined_schema(
            "users",
            vec![("id", DataType::Integer), ("name", DataType::Varchar { max_length: Some(50) })],
            "orders",
            vec![("user_id", DataType::Integer), ("amount", DataType::Integer)],
        );

        let db = Database::default();
        let timeout_ctx = crate::timeout::TimeoutContext::new_default();

        // Condition: users.id = orders.user_id
        let condition = vibesql_ast::Expression::BinaryOp {
            left: Box::new(vibesql_ast::Expression::ColumnRef(
                vibesql_ast::ColumnIdentifier::qualified("users", false, "id", false),
            )),
            op: vibesql_ast::BinaryOperator::Equal,
            right: Box::new(vibesql_ast::Expression::ColumnRef(
                vibesql_ast::ColumnIdentifier::qualified("orders", false, "user_id", false),
            )),
        };

        let result = super::super::parallel::execute_nested_loop_parallel(
            &left_rows,
            &right_rows,
            &Some(condition),
            &schema,
            &db,
            &timeout_ctx,
        )
        .unwrap();

        // Each of 10 keys appears 10 times in left (100/10) and 5 times in right (50/10)
        // So expected result: 10 keys * 10 left_matches * 5 right_matches = 500
        assert_eq!(result.len(), 500);
    }

    #[test]
    fn test_parallel_nested_loop_cross_product() {
        // Test parallel cross product (no condition)
        let left_rows: Vec<Row> = (0..100).map(|i| Row::new(vec![SqlValue::Integer(i)])).collect();

        let right_rows: Vec<Row> = (0..50).map(|i| Row::new(vec![SqlValue::Integer(i)])).collect();

        let schema = create_combined_schema(
            "left",
            vec![("a", DataType::Integer)],
            "right",
            vec![("b", DataType::Integer)],
        );

        let db = Database::default();
        let timeout_ctx = crate::timeout::TimeoutContext::new_default();

        let result = super::super::parallel::execute_nested_loop_parallel(
            &left_rows,
            &right_rows,
            &None,
            &schema,
            &db,
            &timeout_ctx,
        )
        .unwrap();

        // Cross product: 100 x 50 = 5000
        assert_eq!(result.len(), 5000);
    }

    #[test]
    fn test_parallel_nested_loop_empty_input() {
        let left_rows: Vec<Row> = vec![];
        let right_rows: Vec<Row> = (0..50).map(|i| Row::new(vec![SqlValue::Integer(i)])).collect();

        let schema = create_combined_schema(
            "left",
            vec![("a", DataType::Integer)],
            "right",
            vec![("b", DataType::Integer)],
        );

        let db = Database::default();
        let timeout_ctx = crate::timeout::TimeoutContext::new_default();

        let result = super::super::parallel::execute_nested_loop_parallel(
            &left_rows,
            &right_rows,
            &None,
            &schema,
            &db,
            &timeout_ctx,
        )
        .unwrap();

        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_parallel_nested_loop_no_matches() {
        // Test with condition that produces no matches
        let left_rows: Vec<Row> =
            (0..100).map(|i| Row::new(vec![SqlValue::Integer(i), SqlValue::Integer(1)])).collect();

        let right_rows: Vec<Row> = (100..150)
            .map(|i| Row::new(vec![SqlValue::Integer(i), SqlValue::Integer(2)]))
            .collect();

        let schema = create_combined_schema(
            "left",
            vec![("a", DataType::Integer), ("x", DataType::Integer)],
            "right",
            vec![("b", DataType::Integer), ("y", DataType::Integer)],
        );

        let db = Database::default();
        let timeout_ctx = crate::timeout::TimeoutContext::new_default();

        // Condition: left.a = right.b (no matches since ranges don't overlap)
        let condition = vibesql_ast::Expression::BinaryOp {
            left: Box::new(vibesql_ast::Expression::ColumnRef(
                vibesql_ast::ColumnIdentifier::qualified("left", false, "a", false),
            )),
            op: vibesql_ast::BinaryOperator::Equal,
            right: Box::new(vibesql_ast::Expression::ColumnRef(
                vibesql_ast::ColumnIdentifier::qualified("right", false, "b", false),
            )),
        };

        let result = super::super::parallel::execute_nested_loop_parallel(
            &left_rows,
            &right_rows,
            &Some(condition),
            &schema,
            &db,
            &timeout_ctx,
        )
        .unwrap();

        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_parallel_sequential_equivalence() {
        // Verify that parallel and sequential produce the same results
        let left_rows: Vec<Row> = (0..50)
            .map(|i| {
                Row::new(vec![
                    SqlValue::Integer(i % 5),
                    SqlValue::Varchar(arcstr::ArcStr::from(format!("L{}", i))),
                ])
            })
            .collect();

        let right_rows: Vec<Row> = (0..30)
            .map(|i| {
                Row::new(vec![
                    SqlValue::Integer(i % 5),
                    SqlValue::Varchar(arcstr::ArcStr::from(format!("R{}", i))),
                ])
            })
            .collect();

        let schema = create_combined_schema(
            "left",
            vec![("id", DataType::Integer), ("data", DataType::Varchar { max_length: Some(50) })],
            "right",
            vec![("id", DataType::Integer), ("info", DataType::Varchar { max_length: Some(50) })],
        );

        let db = Database::default();
        let timeout_ctx = crate::timeout::TimeoutContext::new_default();

        // Condition: left.id = right.id
        let condition = vibesql_ast::Expression::BinaryOp {
            left: Box::new(vibesql_ast::Expression::ColumnRef(
                vibesql_ast::ColumnIdentifier::qualified("left", false, "id", false),
            )),
            op: vibesql_ast::BinaryOperator::Equal,
            right: Box::new(vibesql_ast::Expression::ColumnRef(
                vibesql_ast::ColumnIdentifier::qualified("right", false, "id", false),
            )),
        };

        // Sequential result
        let seq_result = {
            let evaluator = CombinedExpressionEvaluator::with_database(&schema, &db);
            let mut result = Vec::new();
            for left_row in &left_rows {
                for right_row in &right_rows {
                    let combined = combine_rows(left_row, right_row);
                    evaluator.clear_cse_cache();
                    if let Ok(SqlValue::Boolean(true)) = evaluator.eval(&condition, &combined) {
                        result.push(combined);
                    }
                }
            }
            result
        };

        // Parallel result
        let par_result = super::super::parallel::execute_nested_loop_parallel(
            &left_rows,
            &right_rows,
            &Some(condition.clone()),
            &schema,
            &db,
            &timeout_ctx,
        )
        .unwrap();

        // Should have same number of results
        assert_eq!(
            seq_result.len(),
            par_result.len(),
            "Sequential and parallel should produce same result count"
        );

        // Both should be 50/5 * 30/5 * 5 = 10 * 6 * 5 = 300
        assert_eq!(seq_result.len(), 300);
    }

    #[test]
    fn test_parallel_nested_loop_large_dataset() {
        // Test with a larger dataset to ensure parallel execution is triggered
        let left_rows: Vec<Row> = (0..10000)
            .map(|i| Row::new(vec![SqlValue::Integer(i % 100), SqlValue::Integer(i)]))
            .collect();

        let right_rows: Vec<Row> = (0..1000)
            .map(|i| Row::new(vec![SqlValue::Integer(i % 100), SqlValue::Integer(i)]))
            .collect();

        let schema = create_combined_schema(
            "left",
            vec![("key", DataType::Integer), ("id", DataType::Integer)],
            "right",
            vec![("key", DataType::Integer), ("id", DataType::Integer)],
        );

        let db = Database::default();
        // Use a generous timeout budget instead of `new_default()`'s 300s.
        // This is a synthetic large-dataset stress test (10,000 x 1,000 rows),
        // not a timeout-behavior test, so it shouldn't be racing a fixed
        // wall-clock budget. On a heavily loaded shared build host, rayon's
        // worker threads can get CPU-starved even while pegged near 100% of
        // their own thread time, causing the 300s default to fire as a
        // spurious failure unrelated to any real regression (#6667).
        let timeout_ctx = crate::timeout::TimeoutContext::new(instant::Instant::now(), 3600);

        // Condition: left.key = right.key
        let condition = vibesql_ast::Expression::BinaryOp {
            left: Box::new(vibesql_ast::Expression::ColumnRef(
                vibesql_ast::ColumnIdentifier::qualified("left", false, "key", false),
            )),
            op: vibesql_ast::BinaryOperator::Equal,
            right: Box::new(vibesql_ast::Expression::ColumnRef(
                vibesql_ast::ColumnIdentifier::qualified("right", false, "key", false),
            )),
        };

        let result = super::super::parallel::execute_nested_loop_parallel(
            &left_rows,
            &right_rows,
            &Some(condition),
            &schema,
            &db,
            &timeout_ctx,
        )
        .unwrap();

        // Each key (0-99) appears 100 times in left and 10 times in right
        // Expected: 100 keys * 100 * 10 = 100,000
        assert_eq!(result.len(), 100_000);
    }
}
