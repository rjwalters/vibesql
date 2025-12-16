use vibesql_ast::Expression;
use vibesql_storage::Row;
use vibesql_types::SqlValue;

use super::*;

fn make_test_rows(values: Vec<i64>) -> Vec<Row> {
    values.into_iter().map(|v| Row::new(vec![SqlValue::Integer(v)])).collect()
}

// ===== COUNT Tests =====

#[test]
fn test_count_star_window() {
    // COUNT(*) over entire partition
    let partition = Partition::new(make_test_rows(vec![10, 20, 30, 40, 50]));
    let frame = 0..5; // All rows

    let result = evaluate_count_window(&partition, &frame, None, evaluate_expression);

    assert_eq!(result, SqlValue::Numeric(5.0));
}

#[test]
fn test_count_window_with_frame() {
    // COUNT(*) over 3-row moving window
    let partition = Partition::new(make_test_rows(vec![1, 2, 3, 4, 5]));

    // Frame: rows 1, 2, 3 (3 rows)
    let frame = 1..4;
    let result = evaluate_count_window(&partition, &frame, None, evaluate_expression);

    assert_eq!(result, SqlValue::Numeric(3.0));
}

#[test]
fn test_count_expr_window() {
    // COUNT(expr) - counts non-NULL values
    let partition = Partition::new(make_test_rows(vec![10, 20, 30]));
    let frame = 0..3;

    let expr = Expression::ColumnRef { schema: None, table: None, column: "0".to_string() };

    let result = evaluate_count_window(&partition, &frame, Some(&expr), evaluate_expression);

    // All 3 values are non-NULL
    assert_eq!(result, SqlValue::Numeric(3.0));
}

// ===== SUM Tests =====

#[test]
fn test_sum_window_running_total() {
    // SUM for running total
    let partition = Partition::new(make_test_rows(vec![10, 20, 30, 40, 50]));

    let expr = Expression::ColumnRef { schema: None, table: None, column: "0".to_string() };

    // Running total at position 2: sum of rows 0, 1, 2
    let frame = 0..3; // 10 + 20 + 30 = 60
    let result = evaluate_sum_window(&partition, &frame, &expr, evaluate_expression);

    assert_eq!(result, SqlValue::Numeric(60.0));
}

#[test]
fn test_sum_window_moving() {
    // SUM over 3-row moving window
    let partition = Partition::new(make_test_rows(vec![5, 10, 15, 20, 25]));

    let expr = Expression::ColumnRef { schema: None, table: None, column: "0".to_string() };

    // Frame: rows 2, 3, 4 (values 15, 20, 25)
    let frame = 2..5;
    let result = evaluate_sum_window(&partition, &frame, &expr, evaluate_expression);

    assert_eq!(result, SqlValue::Numeric(60.0)); // 15 + 20 + 25
}

#[test]
fn test_sum_window_empty_frame() {
    let partition = Partition::new(make_test_rows(vec![10, 20, 30]));

    let expr = Expression::ColumnRef { schema: None, table: None, column: "0".to_string() };

    // Empty frame
    let frame = 0..0;
    let result = evaluate_sum_window(&partition, &frame, &expr, evaluate_expression);

    assert_eq!(result, SqlValue::Null);
}

// ===== AVG Tests =====

#[test]
fn test_avg_window_simple() {
    // AVG over entire partition
    let partition = Partition::new(make_test_rows(vec![10, 20, 30, 40, 50]));

    let expr = Expression::ColumnRef { schema: None, table: None, column: "0".to_string() };

    let frame = 0..5;
    let result = evaluate_avg_window(&partition, &frame, &expr, evaluate_expression);

    // Average: (10 + 20 + 30 + 40 + 50) / 5 = 30
    assert_eq!(result, SqlValue::Numeric(30.0));
}

#[test]
fn test_avg_window_moving() {
    // 3-row moving average
    let partition = Partition::new(make_test_rows(vec![10, 20, 30, 40, 50]));

    let expr = Expression::ColumnRef { schema: None, table: None, column: "0".to_string() };

    // Frame: rows 1, 2, 3 (values 20, 30, 40)
    let frame = 1..4;
    let result = evaluate_avg_window(&partition, &frame, &expr, evaluate_expression);

    // Average: (20 + 30 + 40) / 3 = 30
    assert_eq!(result, SqlValue::Numeric(30.0));
}

#[test]
fn test_avg_window_empty_frame() {
    let partition = Partition::new(make_test_rows(vec![10, 20, 30]));

    let expr = Expression::ColumnRef { schema: None, table: None, column: "0".to_string() };

    // Empty frame
    let frame = 0..0;
    let result = evaluate_avg_window(&partition, &frame, &expr, evaluate_expression);

    assert_eq!(result, SqlValue::Null);
}

// ===== MIN Tests =====

#[test]
fn test_min_window_partition() {
    // MIN over entire partition
    let partition = Partition::new(make_test_rows(vec![50, 20, 80, 10, 40]));

    let expr = Expression::ColumnRef { schema: None, table: None, column: "0".to_string() };

    let frame = 0..5;
    let result = evaluate_min_window(&partition, &frame, &expr, evaluate_expression);

    assert_eq!(result, SqlValue::Integer(10));
}

#[test]
fn test_min_window_moving() {
    // MIN in 3-row window
    let partition = Partition::new(make_test_rows(vec![50, 20, 80, 10, 40]));

    let expr = Expression::ColumnRef { schema: None, table: None, column: "0".to_string() };

    // Frame: rows 1, 2, 3 (values 20, 80, 10)
    let frame = 1..4;
    let result = evaluate_min_window(&partition, &frame, &expr, evaluate_expression);

    assert_eq!(result, SqlValue::Integer(10));
}

#[test]
fn test_min_window_empty_frame() {
    let partition = Partition::new(make_test_rows(vec![10, 20, 30]));

    let expr = Expression::ColumnRef { schema: None, table: None, column: "0".to_string() };

    // Empty frame
    let frame = 0..0;
    let result = evaluate_min_window(&partition, &frame, &expr, evaluate_expression);

    assert_eq!(result, SqlValue::Null);
}

// ===== MAX Tests =====

#[test]
fn test_max_window_partition() {
    // MAX over entire partition
    let partition = Partition::new(make_test_rows(vec![50, 20, 80, 10, 40]));

    let expr = Expression::ColumnRef { schema: None, table: None, column: "0".to_string() };

    let frame = 0..5;
    let result = evaluate_max_window(&partition, &frame, &expr, evaluate_expression);

    assert_eq!(result, SqlValue::Integer(80));
}

#[test]
fn test_max_window_moving() {
    // MAX in 3-row window
    let partition = Partition::new(make_test_rows(vec![50, 20, 80, 10, 40]));

    let expr = Expression::ColumnRef { schema: None, table: None, column: "0".to_string() };

    // Frame: rows 2, 3, 4 (values 80, 10, 40)
    let frame = 2..5;
    let result = evaluate_max_window(&partition, &frame, &expr, evaluate_expression);

    assert_eq!(result, SqlValue::Integer(80));
}

#[test]
fn test_max_window_empty_frame() {
    let partition = Partition::new(make_test_rows(vec![10, 20, 30]));

    let expr = Expression::ColumnRef { schema: None, table: None, column: "0".to_string() };

    // Empty frame
    let frame = 0..0;
    let result = evaluate_max_window(&partition, &frame, &expr, evaluate_expression);

    assert_eq!(result, SqlValue::Null);
}

// ===== Frame Boundary Tests =====

#[test]
fn test_frame_at_partition_boundaries() {
    // Test frame behavior at partition start and end
    let partition = Partition::new(make_test_rows(vec![10, 20, 30, 40, 50]));

    let expr = Expression::ColumnRef { schema: None, table: None, column: "0".to_string() };

    // Frame extends beyond partition end
    let frame = 3..10; // Should clamp to 3..5
    let result = evaluate_sum_window(&partition, &frame, &expr, evaluate_expression);

    // Should only sum rows 3, 4 (values 40, 50)
    assert_eq!(result, SqlValue::Numeric(90.0));
}
