use vibesql_ast::{Expression, OrderByItem, OrderDirection};
use vibesql_storage::Row;
use vibesql_types::SqlValue;

use super::*;

fn make_test_rows(values: Vec<i64>) -> Vec<Row> {
    values.into_iter().map(|v| Row::new(vec![SqlValue::Integer(v)])).collect()
}

// ===== ROW_NUMBER Tests =====

#[test]
fn test_row_number_simple() {
    let partition = Partition::new(make_test_rows(vec![10, 20, 30, 40, 50]));

    let result = evaluate_row_number(&partition);

    assert_eq!(result.len(), 5);
    assert_eq!(result[0], SqlValue::Integer(1));
    assert_eq!(result[1], SqlValue::Integer(2));
    assert_eq!(result[2], SqlValue::Integer(3));
    assert_eq!(result[3], SqlValue::Integer(4));
    assert_eq!(result[4], SqlValue::Integer(5));
}

// ===== RANK Tests =====

#[test]
fn test_rank_with_ties() {
    // Scores: 95, 90, 90, 85
    // Expected ranks: 1, 2, 2, 4
    let partition = Partition::new(make_test_rows(vec![95, 90, 90, 85]));

    let order_by = Some(vec![OrderByItem {
        expr: Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("", false)),
        direction: OrderDirection::Desc,
        nulls_order: None,
    }]);

    let result = evaluate_rank(&partition, &order_by);

    assert_eq!(result.len(), 4);
    assert_eq!(result[0], SqlValue::Integer(1)); // 95 -> rank 1
    assert_eq!(result[1], SqlValue::Integer(2)); // 90 -> rank 2
    assert_eq!(result[2], SqlValue::Integer(2)); // 90 -> rank 2 (tie)
    assert_eq!(result[3], SqlValue::Integer(4)); // 85 -> rank 4 (gap at 3)
}

#[test]
fn test_rank_no_ties() {
    let partition = Partition::new(make_test_rows(vec![4, 3, 2, 1]));

    let order_by = Some(vec![OrderByItem {
        expr: Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("", false)),
        direction: OrderDirection::Desc,
        nulls_order: None,
    }]);

    let result = evaluate_rank(&partition, &order_by);

    assert_eq!(result.len(), 4);
    assert_eq!(result[0], SqlValue::Integer(1));
    assert_eq!(result[1], SqlValue::Integer(2));
    assert_eq!(result[2], SqlValue::Integer(3));
    assert_eq!(result[3], SqlValue::Integer(4));
}

#[test]
fn test_rank_without_order_by() {
    let partition = Partition::new(make_test_rows(vec![10, 20, 30]));

    let result = evaluate_rank(&partition, &None);

    // Without ORDER BY, all rows get rank 1
    assert_eq!(result.len(), 3);
    assert_eq!(result[0], SqlValue::Integer(1));
    assert_eq!(result[1], SqlValue::Integer(1));
    assert_eq!(result[2], SqlValue::Integer(1));
}

// ===== DENSE_RANK Tests =====

#[test]
fn test_dense_rank_with_ties() {
    // Scores: 95, 90, 90, 85
    // Expected ranks: 1, 2, 2, 3 (no gap)
    let partition = Partition::new(make_test_rows(vec![95, 90, 90, 85]));

    let order_by = Some(vec![OrderByItem {
        expr: Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("", false)),
        direction: OrderDirection::Desc,
        nulls_order: None,
    }]);

    let result = evaluate_dense_rank(&partition, &order_by);

    assert_eq!(result.len(), 4);
    assert_eq!(result[0], SqlValue::Integer(1)); // 95 -> rank 1
    assert_eq!(result[1], SqlValue::Integer(2)); // 90 -> rank 2
    assert_eq!(result[2], SqlValue::Integer(2)); // 90 -> rank 2 (tie)
    assert_eq!(result[3], SqlValue::Integer(3)); // 85 -> rank 3 (no gap)
}

#[test]
fn test_dense_rank_multiple_tie_groups() {
    // Scores: 100, 90, 90, 80, 80, 80, 70
    // Expected ranks: 1, 2, 2, 3, 3, 3, 4
    let partition = Partition::new(make_test_rows(vec![100, 90, 90, 80, 80, 80, 70]));

    let order_by = Some(vec![OrderByItem {
        expr: Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("", false)),
        direction: OrderDirection::Desc,
        nulls_order: None,
    }]);

    let result = evaluate_dense_rank(&partition, &order_by);

    assert_eq!(result.len(), 7);
    assert_eq!(result[0], SqlValue::Integer(1));
    assert_eq!(result[1], SqlValue::Integer(2));
    assert_eq!(result[2], SqlValue::Integer(2));
    assert_eq!(result[3], SqlValue::Integer(3));
    assert_eq!(result[4], SqlValue::Integer(3));
    assert_eq!(result[5], SqlValue::Integer(3));
    assert_eq!(result[6], SqlValue::Integer(4));
}

// ===== NTILE Tests =====

#[test]
fn test_ntile_even_division() {
    // 8 rows, NTILE(4) -> 2 rows per bucket
    let partition = Partition::new(make_test_rows(vec![1, 2, 3, 4, 5, 6, 7, 8]));

    let result = evaluate_ntile(&partition, 4).unwrap();

    assert_eq!(result.len(), 8);
    assert_eq!(result[0], SqlValue::Integer(1));
    assert_eq!(result[1], SqlValue::Integer(1));
    assert_eq!(result[2], SqlValue::Integer(2));
    assert_eq!(result[3], SqlValue::Integer(2));
    assert_eq!(result[4], SqlValue::Integer(3));
    assert_eq!(result[5], SqlValue::Integer(3));
    assert_eq!(result[6], SqlValue::Integer(4));
    assert_eq!(result[7], SqlValue::Integer(4));
}

#[test]
fn test_ntile_uneven_division() {
    // 10 rows, NTILE(4) -> buckets of 3, 3, 2, 2
    let partition = Partition::new(make_test_rows(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]));

    let result = evaluate_ntile(&partition, 4).unwrap();

    assert_eq!(result.len(), 10);
    // First bucket: 3 rows (base_size=2 + 1)
    assert_eq!(result[0], SqlValue::Integer(1));
    assert_eq!(result[1], SqlValue::Integer(1));
    assert_eq!(result[2], SqlValue::Integer(1));
    // Second bucket: 3 rows
    assert_eq!(result[3], SqlValue::Integer(2));
    assert_eq!(result[4], SqlValue::Integer(2));
    assert_eq!(result[5], SqlValue::Integer(2));
    // Third bucket: 2 rows (base_size)
    assert_eq!(result[6], SqlValue::Integer(3));
    assert_eq!(result[7], SqlValue::Integer(3));
    // Fourth bucket: 2 rows
    assert_eq!(result[8], SqlValue::Integer(4));
    assert_eq!(result[9], SqlValue::Integer(4));
}

#[test]
fn test_ntile_n_greater_than_rows() {
    // 3 rows, NTILE(5) -> each row gets own bucket
    let partition = Partition::new(make_test_rows(vec![1, 2, 3]));

    let result = evaluate_ntile(&partition, 5).unwrap();

    assert_eq!(result.len(), 3);
    assert_eq!(result[0], SqlValue::Integer(1));
    assert_eq!(result[1], SqlValue::Integer(2));
    assert_eq!(result[2], SqlValue::Integer(3));
}

#[test]
fn test_ntile_single_bucket() {
    let partition = Partition::new(make_test_rows(vec![1, 2, 3, 4, 5]));

    let result = evaluate_ntile(&partition, 1).unwrap();

    assert_eq!(result.len(), 5);
    // All rows in bucket 1
    for val in result {
        assert_eq!(val, SqlValue::Integer(1));
    }
}

#[test]
fn test_ntile_invalid_argument() {
    let partition = Partition::new(make_test_rows(vec![1, 2, 3]));

    let result = evaluate_ntile(&partition, 0);
    assert!(result.is_err());

    let result = evaluate_ntile(&partition, -1);
    assert!(result.is_err());
}
