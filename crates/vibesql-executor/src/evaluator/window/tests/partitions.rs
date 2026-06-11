use vibesql_storage::Row;
use vibesql_types::SqlValue;

use super::*;

fn make_test_rows(values: Vec<i64>) -> Vec<Row> {
    values.into_iter().map(|v| Row::new(vec![SqlValue::Integer(v)])).collect()
}

#[test]
fn test_partition_rows_no_partition_by() {
    let rows = make_test_rows(vec![1, 2, 3]);
    let partitions = partition_rows(rows, &None, evaluate_expression);

    assert_eq!(partitions.len(), 1);
    assert_eq!(partitions[0].len(), 3);
}

#[test]
fn test_partition_rows_empty_partition_by() {
    let rows = make_test_rows(vec![1, 2, 3]);
    let partitions = partition_rows(rows, &Some(vec![]), evaluate_expression);

    assert_eq!(partitions.len(), 1);
    assert_eq!(partitions[0].len(), 3);
}

// ===== ORDER BY NULLS FIRST/LAST in partition sort (#5191) =====

/// Extract the first-column values of a partition's rows
fn first_col_values(partition: &Partition) -> Vec<SqlValue> {
    partition.rows.iter().map(|r| r.values[0].clone()).collect()
}

fn sort_test_partition() -> Partition {
    Partition::new(vec![
        Row::new(vec![SqlValue::Integer(10)]),
        Row::new(vec![SqlValue::Null]),
        Row::new(vec![SqlValue::Integer(5)]),
        Row::new(vec![SqlValue::Null]),
    ])
}

fn nulls_order_by(
    direction: vibesql_ast::OrderDirection,
    nulls_order: Option<vibesql_ast::NullsOrder>,
) -> Option<Vec<vibesql_ast::OrderByItem>> {
    Some(vec![vibesql_ast::OrderByItem {
        expr: vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("0", false)),
        direction,
        nulls_order,
    }])
}

#[test]
fn test_sort_partition_asc_nulls_last() {
    let mut partition = sort_test_partition();
    let order_by =
        nulls_order_by(vibesql_ast::OrderDirection::Asc, Some(vibesql_ast::NullsOrder::Last));
    sort_partition(&mut partition, &order_by, evaluate_expression);

    assert_eq!(
        first_col_values(&partition),
        vec![SqlValue::Integer(5), SqlValue::Integer(10), SqlValue::Null, SqlValue::Null]
    );
}

#[test]
fn test_sort_partition_desc_nulls_first() {
    let mut partition = sort_test_partition();
    let order_by =
        nulls_order_by(vibesql_ast::OrderDirection::Desc, Some(vibesql_ast::NullsOrder::First));
    sort_partition(&mut partition, &order_by, evaluate_expression);

    assert_eq!(
        first_col_values(&partition),
        vec![SqlValue::Null, SqlValue::Null, SqlValue::Integer(10), SqlValue::Integer(5)]
    );
}

#[test]
fn test_sort_partition_default_null_placement() {
    // Defaults (no NULLS modifier): NULLS FIRST for ASC, NULLS LAST for DESC
    let mut partition = sort_test_partition();
    let order_by = nulls_order_by(vibesql_ast::OrderDirection::Asc, None);
    sort_partition(&mut partition, &order_by, evaluate_expression);
    assert_eq!(
        first_col_values(&partition),
        vec![SqlValue::Null, SqlValue::Null, SqlValue::Integer(5), SqlValue::Integer(10)]
    );

    let mut partition = sort_test_partition();
    let order_by = nulls_order_by(vibesql_ast::OrderDirection::Desc, None);
    sort_partition(&mut partition, &order_by, evaluate_expression);
    assert_eq!(
        first_col_values(&partition),
        vec![SqlValue::Integer(10), SqlValue::Integer(5), SqlValue::Null, SqlValue::Null]
    );
}
