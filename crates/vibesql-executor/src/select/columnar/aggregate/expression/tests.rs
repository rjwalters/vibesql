//! Tests for expression aggregate modules

use std::sync::Arc;

use crate::schema::CombinedSchema;
use vibesql_ast::BinaryOperator;
use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_types::DataType;

use crate::select::columnar::batch::{ColumnArray, ColumnarBatch};

fn make_test_schema() -> CombinedSchema {
    let schema = TableSchema::new(
        "test".to_string(),
        vec![
            ColumnSchema::new("price".to_string(), DataType::DoublePrecision, false),
            ColumnSchema::new("discount".to_string(), DataType::DoublePrecision, false),
            ColumnSchema::new("quantity".to_string(), DataType::Integer, false),
        ],
    );
    CombinedSchema::from_table("test".to_string(), schema)
}

fn make_test_batch() -> ColumnarBatch {
    // Create a batch with price, discount, and quantity columns
    let price_col = ColumnArray::Float64(Arc::new(vec![100.0, 200.0, 300.0, 400.0]), None);
    let discount_col = ColumnArray::Float64(Arc::new(vec![0.1, 0.2, 0.15, 0.05]), None);
    let quantity_col = ColumnArray::Int64(Arc::new(vec![10, 20, 15, 25]), None);

    ColumnarBatch::from_columns(
        vec![price_col, discount_col, quantity_col],
        Some(vec!["price".to_string(), "discount".to_string(), "quantity".to_string()]),
    )
    .unwrap()
}

#[test]
fn test_batch_expression_aggregate_multiply() {
    use super::batch::compute_batch_expression_aggregate;
    use crate::select::columnar::aggregate::AggregateOp;
    use vibesql_ast::Expression;
    use vibesql_types::SqlValue;

    let batch = make_test_batch();
    let schema = make_test_schema();

    // Test SUM(price * discount)
    let expr = Expression::BinaryOp {
        left: Box::new(Expression::ColumnRef { table: None, column: "price".to_string() }),
        op: BinaryOperator::Multiply,
        right: Box::new(Expression::ColumnRef { table: None, column: "discount".to_string() }),
    };

    let result = compute_batch_expression_aggregate(&batch, &expr, AggregateOp::Sum, &schema)
        .expect("Should compute batch expression aggregate");

    // Expected: 100*0.1 + 200*0.2 + 300*0.15 + 400*0.05 = 10 + 40 + 45 + 20 = 115
    match result {
        SqlValue::Double(sum) => {
            assert!((sum - 115.0).abs() < 0.001, "Expected 115.0, got {}", sum);
        }
        other => panic!("Expected Double, got {:?}", other),
    }
}

#[test]
fn test_batch_expression_aggregate_avg() {
    use super::batch::compute_batch_expression_aggregate;
    use crate::select::columnar::aggregate::AggregateOp;
    use vibesql_ast::Expression;
    use vibesql_types::SqlValue;

    let batch = make_test_batch();
    let schema = make_test_schema();

    // Test AVG(price * discount)
    let expr = Expression::BinaryOp {
        left: Box::new(Expression::ColumnRef { table: None, column: "price".to_string() }),
        op: BinaryOperator::Multiply,
        right: Box::new(Expression::ColumnRef { table: None, column: "discount".to_string() }),
    };

    let result = compute_batch_expression_aggregate(&batch, &expr, AggregateOp::Avg, &schema)
        .expect("Should compute batch expression aggregate");

    // Expected: 115.0 / 4 = 28.75
    match result {
        SqlValue::Double(avg) => {
            assert!((avg - 28.75).abs() < 0.001, "Expected 28.75, got {}", avg);
        }
        other => panic!("Expected Double, got {:?}", other),
    }
}

#[test]
fn test_batch_expression_aggregate_mixed_types() {
    use super::batch::compute_batch_expression_aggregate;
    use crate::select::columnar::aggregate::AggregateOp;
    use vibesql_ast::Expression;
    use vibesql_types::SqlValue;

    let batch = make_test_batch();
    let schema = make_test_schema();

    // Test SUM(price * quantity) - Float64 * Int64
    let expr = Expression::BinaryOp {
        left: Box::new(Expression::ColumnRef { table: None, column: "price".to_string() }),
        op: BinaryOperator::Multiply,
        right: Box::new(Expression::ColumnRef { table: None, column: "quantity".to_string() }),
    };

    let result = compute_batch_expression_aggregate(&batch, &expr, AggregateOp::Sum, &schema)
        .expect("Should compute batch expression aggregate");

    // Expected: 100*10 + 200*20 + 300*15 + 400*25 = 1000 + 4000 + 4500 + 10000 = 19500
    match result {
        SqlValue::Double(sum) => {
            assert!((sum - 19500.0).abs() < 0.001, "Expected 19500.0, got {}", sum);
        }
        other => panic!("Expected Double, got {:?}", other),
    }
}

#[test]
fn test_batch_expression_aggregate_with_literal() {
    use super::batch::compute_batch_expression_aggregate;
    use crate::select::columnar::aggregate::AggregateOp;
    use vibesql_ast::Expression;
    use vibesql_types::SqlValue;

    let batch = make_test_batch();
    let schema = make_test_schema();

    // Test SUM(price * 2)
    let expr = Expression::BinaryOp {
        left: Box::new(Expression::ColumnRef { table: None, column: "price".to_string() }),
        op: BinaryOperator::Multiply,
        right: Box::new(Expression::Literal(SqlValue::Integer(2))),
    };

    let result = compute_batch_expression_aggregate(&batch, &expr, AggregateOp::Sum, &schema)
        .expect("Should compute batch expression aggregate");

    // Expected: (100 + 200 + 300 + 400) * 2 = 2000
    match result {
        SqlValue::Double(sum) => {
            assert!((sum - 2000.0).abs() < 0.001, "Expected 2000.0, got {}", sum);
        }
        other => panic!("Expected Double, got {:?}", other),
    }
}

#[test]
fn test_batch_expression_aggregate_nested() {
    use super::batch::compute_batch_expression_aggregate;
    use crate::select::columnar::aggregate::AggregateOp;
    use vibesql_ast::Expression;
    use vibesql_types::SqlValue;

    let batch = make_test_batch();
    let schema = make_test_schema();

    // Test SUM(price * (1 - discount))
    let expr = Expression::BinaryOp {
        left: Box::new(Expression::ColumnRef { table: None, column: "price".to_string() }),
        op: BinaryOperator::Multiply,
        right: Box::new(Expression::BinaryOp {
            left: Box::new(Expression::Literal(SqlValue::Double(1.0))),
            op: BinaryOperator::Minus,
            right: Box::new(Expression::ColumnRef { table: None, column: "discount".to_string() }),
        }),
    };

    let result = compute_batch_expression_aggregate(&batch, &expr, AggregateOp::Sum, &schema)
        .expect("Should compute batch expression aggregate");

    // Expected: 100*0.9 + 200*0.8 + 300*0.85 + 400*0.95 = 90 + 160 + 255 + 380 = 885
    match result {
        SqlValue::Double(sum) => {
            assert!((sum - 885.0).abs() < 0.001, "Expected 885.0, got {}", sum);
        }
        other => panic!("Expected Double, got {:?}", other),
    }
}

#[test]
fn test_batch_expression_aggregate_min_max() {
    use super::batch::compute_batch_expression_aggregate;
    use crate::select::columnar::aggregate::AggregateOp;
    use vibesql_ast::Expression;
    use vibesql_types::SqlValue;

    let batch = make_test_batch();
    let schema = make_test_schema();

    // Test MIN(price * discount)
    let expr = Expression::BinaryOp {
        left: Box::new(Expression::ColumnRef { table: None, column: "price".to_string() }),
        op: BinaryOperator::Multiply,
        right: Box::new(Expression::ColumnRef { table: None, column: "discount".to_string() }),
    };

    let min_result = compute_batch_expression_aggregate(&batch, &expr, AggregateOp::Min, &schema)
        .expect("Should compute MIN");
    let max_result = compute_batch_expression_aggregate(&batch, &expr, AggregateOp::Max, &schema)
        .expect("Should compute MAX");

    // Values: 10, 40, 45, 20
    // Min: 10, Max: 45
    match min_result {
        SqlValue::Double(min) => {
            assert!((min - 10.0).abs() < 0.001, "Expected MIN 10.0, got {}", min);
        }
        other => panic!("Expected Double for MIN, got {:?}", other),
    }

    match max_result {
        SqlValue::Double(max) => {
            assert!((max - 45.0).abs() < 0.001, "Expected MAX 45.0, got {}", max);
        }
        other => panic!("Expected Double for MAX, got {:?}", other),
    }
}

#[test]
fn test_batch_expression_aggregate_empty_batch() {
    use super::batch::compute_batch_expression_aggregate;
    use crate::select::columnar::aggregate::AggregateOp;
    use vibesql_ast::Expression;
    use vibesql_types::SqlValue;

    let batch = ColumnarBatch::from_columns(
        vec![
            ColumnArray::Float64(Arc::new(vec![]), None),
            ColumnArray::Float64(Arc::new(vec![]), None),
        ],
        Some(vec!["price".to_string(), "discount".to_string()]),
    )
    .unwrap();

    let schema = TableSchema::new(
        "test".to_string(),
        vec![
            ColumnSchema::new("price".to_string(), DataType::DoublePrecision, false),
            ColumnSchema::new("discount".to_string(), DataType::DoublePrecision, false),
        ],
    );
    let combined_schema = CombinedSchema::from_table("test".to_string(), schema);

    let expr = Expression::BinaryOp {
        left: Box::new(Expression::ColumnRef { table: None, column: "price".to_string() }),
        op: BinaryOperator::Multiply,
        right: Box::new(Expression::ColumnRef { table: None, column: "discount".to_string() }),
    };

    // SUM of empty batch should return NULL
    let sum_result =
        compute_batch_expression_aggregate(&batch, &expr, AggregateOp::Sum, &combined_schema)
            .expect("Should handle empty batch");
    assert_eq!(sum_result, SqlValue::Null);

    // COUNT of empty batch should return 0
    let count_result =
        compute_batch_expression_aggregate(&batch, &expr, AggregateOp::Count, &combined_schema)
            .expect("Should handle empty batch");
    assert_eq!(count_result, SqlValue::Integer(0));
}

#[test]
fn test_batch_expression_aggregate_with_nulls() {
    use super::batch::compute_batch_expression_aggregate;
    use crate::select::columnar::aggregate::AggregateOp;
    use vibesql_ast::Expression;
    use vibesql_types::SqlValue;

    // Create a batch with some NULL values
    let price_col = ColumnArray::Float64(
        Arc::new(vec![100.0, 200.0, 300.0, 400.0]),
        Some(Arc::new(vec![false, true, false, false])), // Second value is NULL
    );
    let discount_col = ColumnArray::Float64(Arc::new(vec![0.1, 0.2, 0.15, 0.05]), None);

    let batch = ColumnarBatch::from_columns(
        vec![price_col, discount_col],
        Some(vec!["price".to_string(), "discount".to_string()]),
    )
    .unwrap();

    let schema = TableSchema::new(
        "test".to_string(),
        vec![
            ColumnSchema::new("price".to_string(), DataType::DoublePrecision, false),
            ColumnSchema::new("discount".to_string(), DataType::DoublePrecision, false),
        ],
    );
    let combined_schema = CombinedSchema::from_table("test".to_string(), schema);

    let expr = Expression::BinaryOp {
        left: Box::new(Expression::ColumnRef { table: None, column: "price".to_string() }),
        op: BinaryOperator::Multiply,
        right: Box::new(Expression::ColumnRef { table: None, column: "discount".to_string() }),
    };

    let result =
        compute_batch_expression_aggregate(&batch, &expr, AggregateOp::Sum, &combined_schema)
            .expect("Should handle batch with nulls");

    // Expected: 100*0.1 + 300*0.15 + 400*0.05 = 10 + 45 + 20 = 75 (skipping NULL row)
    match result {
        SqlValue::Double(sum) => {
            assert!((sum - 75.0).abs() < 0.001, "Expected 75.0, got {}", sum);
        }
        other => panic!("Expected Double, got {:?}", other),
    }
}
