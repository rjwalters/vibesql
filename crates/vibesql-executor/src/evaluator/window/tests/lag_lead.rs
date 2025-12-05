use vibesql_ast::Expression;
use vibesql_storage::Row;
use vibesql_types::SqlValue;

use super::*;

fn make_test_rows(values: Vec<i64>) -> Vec<Row> {
    values.into_iter().map(|v| Row::new(vec![SqlValue::Integer(v)])).collect()
}

// Simple evaluation function for tests
fn simple_eval(expr: &Expression, row: &Row) -> Result<SqlValue, String> {
    match expr {
        Expression::Literal(val) => Ok(val.clone()),
        Expression::ColumnRef { column, .. } => {
            let col_idx: usize =
                column.parse().map_err(|e| format!("Invalid column index: {}", e))?;
            row.get(col_idx)
                .cloned()
                .ok_or_else(|| format!("Column index {} out of bounds", col_idx))
        }
        _ => Err("Unsupported expression in test".to_string()),
    }
}

// ===== LAG Tests =====

#[test]
fn test_lag_default_offset() {
    // LAG(value) with default offset of 1
    let partition = Partition::new(make_test_rows(vec![10, 20, 30, 40, 50]));

    let value_expr = Expression::ColumnRef { table: None, column: "0".to_string() };

    // Row 0: LAG should return NULL (no previous row)
    let result = evaluate_lag(&partition, 0, &value_expr, None, None, simple_eval).unwrap();
    assert_eq!(result, SqlValue::Null);

    // Row 1: LAG should return 10 (previous row value)
    let result = evaluate_lag(&partition, 1, &value_expr, None, None, simple_eval).unwrap();
    assert_eq!(result, SqlValue::Integer(10));

    // Row 2: LAG should return 20
    let result = evaluate_lag(&partition, 2, &value_expr, None, None, simple_eval).unwrap();
    assert_eq!(result, SqlValue::Integer(20));

    // Row 4: LAG should return 40
    let result = evaluate_lag(&partition, 4, &value_expr, None, None, simple_eval).unwrap();
    assert_eq!(result, SqlValue::Integer(40));
}

#[test]
fn test_lag_custom_offset() {
    // LAG(value, 2) - look back 2 rows
    let partition = Partition::new(make_test_rows(vec![10, 20, 30, 40, 50]));

    let value_expr = Expression::ColumnRef { table: None, column: "0".to_string() };

    // Row 0: offset 2 goes before partition start -> NULL
    let result = evaluate_lag(&partition, 0, &value_expr, Some(2), None, simple_eval).unwrap();
    assert_eq!(result, SqlValue::Null);

    // Row 1: offset 2 goes before partition start -> NULL
    let result = evaluate_lag(&partition, 1, &value_expr, Some(2), None, simple_eval).unwrap();
    assert_eq!(result, SqlValue::Null);

    // Row 2: LAG(value, 2) should return 10 (row 0)
    let result = evaluate_lag(&partition, 2, &value_expr, Some(2), None, simple_eval).unwrap();
    assert_eq!(result, SqlValue::Integer(10));

    // Row 3: LAG(value, 2) should return 20 (row 1)
    let result = evaluate_lag(&partition, 3, &value_expr, Some(2), None, simple_eval).unwrap();
    assert_eq!(result, SqlValue::Integer(20));

    // Row 4: LAG(value, 2) should return 30 (row 2)
    let result = evaluate_lag(&partition, 4, &value_expr, Some(2), None, simple_eval).unwrap();
    assert_eq!(result, SqlValue::Integer(30));
}

#[test]
fn test_lag_with_default_value() {
    // LAG(value, 1, 0) - default value of 0 instead of NULL
    let partition = Partition::new(make_test_rows(vec![10, 20, 30]));

    let value_expr = Expression::ColumnRef { table: None, column: "0".to_string() };

    let default_expr = Expression::Literal(SqlValue::Integer(0));

    // Row 0: should return 0 (default) instead of NULL
    let result =
        evaluate_lag(&partition, 0, &value_expr, None, Some(&default_expr), simple_eval).unwrap();
    assert_eq!(result, SqlValue::Integer(0));

    // Row 1: should return 10 (previous row)
    let result =
        evaluate_lag(&partition, 1, &value_expr, None, Some(&default_expr), simple_eval).unwrap();
    assert_eq!(result, SqlValue::Integer(10));

    // Row 2: should return 20 (previous row)
    let result =
        evaluate_lag(&partition, 2, &value_expr, None, Some(&default_expr), simple_eval).unwrap();
    assert_eq!(result, SqlValue::Integer(20));
}

#[test]
fn test_lag_offset_beyond_partition_start() {
    // Large offset that goes way before partition start
    let partition = Partition::new(make_test_rows(vec![10, 20, 30]));

    let value_expr = Expression::ColumnRef { table: None, column: "0".to_string() };

    // Row 2 with offset 100 should return NULL
    let result = evaluate_lag(&partition, 2, &value_expr, Some(100), None, simple_eval).unwrap();
    assert_eq!(result, SqlValue::Null);

    // With default value
    let default_expr = Expression::Literal(SqlValue::Integer(-1));
    let result =
        evaluate_lag(&partition, 2, &value_expr, Some(100), Some(&default_expr), simple_eval)
            .unwrap();
    assert_eq!(result, SqlValue::Integer(-1));
}

// ===== LEAD Tests =====

#[test]
fn test_lead_default_offset() {
    // LEAD(value) with default offset of 1
    let partition = Partition::new(make_test_rows(vec![10, 20, 30, 40, 50]));

    let value_expr = Expression::ColumnRef { table: None, column: "0".to_string() };

    // Row 0: LEAD should return 20 (next row value)
    let result = evaluate_lead(&partition, 0, &value_expr, None, None, simple_eval).unwrap();
    assert_eq!(result, SqlValue::Integer(20));

    // Row 1: LEAD should return 30
    let result = evaluate_lead(&partition, 1, &value_expr, None, None, simple_eval).unwrap();
    assert_eq!(result, SqlValue::Integer(30));

    // Row 3: LEAD should return 50
    let result = evaluate_lead(&partition, 3, &value_expr, None, None, simple_eval).unwrap();
    assert_eq!(result, SqlValue::Integer(50));

    // Row 4: LEAD should return NULL (no next row)
    let result = evaluate_lead(&partition, 4, &value_expr, None, None, simple_eval).unwrap();
    assert_eq!(result, SqlValue::Null);
}

#[test]
fn test_lead_custom_offset() {
    // LEAD(value, 2) - look forward 2 rows
    let partition = Partition::new(make_test_rows(vec![10, 20, 30, 40, 50]));

    let value_expr = Expression::ColumnRef { table: None, column: "0".to_string() };

    // Row 0: LEAD(value, 2) should return 30 (row 2)
    let result = evaluate_lead(&partition, 0, &value_expr, Some(2), None, simple_eval).unwrap();
    assert_eq!(result, SqlValue::Integer(30));

    // Row 1: LEAD(value, 2) should return 40 (row 3)
    let result = evaluate_lead(&partition, 1, &value_expr, Some(2), None, simple_eval).unwrap();
    assert_eq!(result, SqlValue::Integer(40));

    // Row 2: LEAD(value, 2) should return 50 (row 4)
    let result = evaluate_lead(&partition, 2, &value_expr, Some(2), None, simple_eval).unwrap();
    assert_eq!(result, SqlValue::Integer(50));

    // Row 3: offset 2 goes past partition end -> NULL
    let result = evaluate_lead(&partition, 3, &value_expr, Some(2), None, simple_eval).unwrap();
    assert_eq!(result, SqlValue::Null);

    // Row 4: offset 2 goes past partition end -> NULL
    let result = evaluate_lead(&partition, 4, &value_expr, Some(2), None, simple_eval).unwrap();
    assert_eq!(result, SqlValue::Null);
}

#[test]
fn test_lead_with_default_value() {
    // LEAD(value, 1, 999) - default value of 999 instead of NULL
    let partition = Partition::new(make_test_rows(vec![10, 20, 30]));

    let value_expr = Expression::ColumnRef { table: None, column: "0".to_string() };

    let default_expr = Expression::Literal(SqlValue::Integer(999));

    // Row 0: should return 20 (next row)
    let result =
        evaluate_lead(&partition, 0, &value_expr, None, Some(&default_expr), simple_eval).unwrap();
    assert_eq!(result, SqlValue::Integer(20));

    // Row 1: should return 30 (next row)
    let result =
        evaluate_lead(&partition, 1, &value_expr, None, Some(&default_expr), simple_eval).unwrap();
    assert_eq!(result, SqlValue::Integer(30));

    // Row 2: should return 999 (default) instead of NULL
    let result =
        evaluate_lead(&partition, 2, &value_expr, None, Some(&default_expr), simple_eval).unwrap();
    assert_eq!(result, SqlValue::Integer(999));
}

#[test]
fn test_lead_offset_beyond_partition_end() {
    // Large offset that goes way past partition end
    let partition = Partition::new(make_test_rows(vec![10, 20, 30]));

    let value_expr = Expression::ColumnRef { table: None, column: "0".to_string() };

    // Row 0 with offset 100 should return NULL
    let result = evaluate_lead(&partition, 0, &value_expr, Some(100), None, simple_eval).unwrap();
    assert_eq!(result, SqlValue::Null);

    // With default value
    let default_expr = Expression::Literal(SqlValue::Integer(-1));
    let result =
        evaluate_lead(&partition, 0, &value_expr, Some(100), Some(&default_expr), simple_eval)
            .unwrap();
    assert_eq!(result, SqlValue::Integer(-1));
}

// ===== Combined LAG/LEAD Tests =====

#[test]
fn test_lag_lead_single_row_partition() {
    // Edge case: partition with only one row
    let partition = Partition::new(make_test_rows(vec![42]));

    let value_expr = Expression::ColumnRef { table: None, column: "0".to_string() };

    // LAG on single row should return NULL
    let result = evaluate_lag(&partition, 0, &value_expr, None, None, simple_eval).unwrap();
    assert_eq!(result, SqlValue::Null);

    // LEAD on single row should return NULL
    let result = evaluate_lead(&partition, 0, &value_expr, None, None, simple_eval).unwrap();
    assert_eq!(result, SqlValue::Null);
}

#[test]
fn test_lag_lead_with_zero_offset() {
    // Special case: offset of 0 should return current row value
    let partition = Partition::new(make_test_rows(vec![10, 20, 30]));

    let value_expr = Expression::ColumnRef { table: None, column: "0".to_string() };

    // LAG(value, 0) should return current row value
    let result = evaluate_lag(&partition, 1, &value_expr, Some(0), None, simple_eval).unwrap();
    assert_eq!(result, SqlValue::Integer(20));

    // LEAD(value, 0) should return current row value
    let result = evaluate_lead(&partition, 1, &value_expr, Some(0), None, simple_eval).unwrap();
    assert_eq!(result, SqlValue::Integer(20));
}

// ===== Error Tests =====

#[test]
fn test_lag_negative_offset_error() {
    // LAG with negative offset should return error
    let partition = Partition::new(make_test_rows(vec![10, 20, 30]));

    let value_expr = Expression::ColumnRef { table: None, column: "0".to_string() };

    let result = evaluate_lag(&partition, 1, &value_expr, Some(-1), None, simple_eval);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("non-negative"));
}

#[test]
fn test_lead_negative_offset_error() {
    // LEAD with negative offset should return error
    let partition = Partition::new(make_test_rows(vec![10, 20, 30]));

    let value_expr = Expression::ColumnRef { table: None, column: "0".to_string() };

    let result = evaluate_lead(&partition, 1, &value_expr, Some(-1), None, simple_eval);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("non-negative"));
}
