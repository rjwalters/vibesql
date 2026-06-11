use vibesql_storage::Row;
use vibesql_types::SqlValue;

use super::*;

fn make_test_rows(values: Vec<i64>) -> Vec<Row> {
    values.into_iter().map(|v| Row::new(vec![SqlValue::Integer(v)])).collect()
}

#[test]
fn test_partition_rows_no_partition_by() {
    let rows = make_test_rows(vec![1, 2, 3]);
    let partitions = partition_rows(rows, &None, evaluate_expression).unwrap();

    assert_eq!(partitions.len(), 1);
    assert_eq!(partitions[0].len(), 3);
}

#[test]
fn test_partition_rows_empty_partition_by() {
    let rows = make_test_rows(vec![1, 2, 3]);
    let partitions = partition_rows(rows, &Some(vec![]), evaluate_expression).unwrap();

    assert_eq!(partitions.len(), 1);
    assert_eq!(partitions[0].len(), 3);
}

// ===== Error propagation from PARTITION BY evaluation (#5301) =====

fn partition_by_column(index: &str) -> Option<Vec<vibesql_ast::Expression>> {
    Some(vec![vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
        index, false,
    ))])
}

/// An evaluation error in a PARTITION BY expression must surface as Err,
/// not silently become a NULL partition key (which would collapse all
/// affected rows into one partition and produce wrong window results).
#[test]
fn test_partition_rows_propagates_eval_errors() {
    let rows = make_test_rows(vec![1, 2, 3]);
    let failing_eval = |_expr: &vibesql_ast::Expression, row: &Row| -> Result<SqlValue, String> {
        match row.values[0] {
            SqlValue::Integer(2) => Err("ColumnNotFound: simulated failure".to_string()),
            ref v => Ok(v.clone()),
        }
    };

    let result = partition_rows(rows, &partition_by_column("0"), failing_eval);

    let err = result.expect_err("evaluation error must propagate, not become a NULL key");
    assert!(err.contains("ColumnNotFound"), "error message should be preserved, got: {err}");
}

/// Partition expressions that legitimately evaluate to NULL (Ok(Null), not Err)
/// must keep their previous behavior: all NULL-keyed rows group together.
#[test]
fn test_partition_rows_null_keys_still_group_together() {
    let rows = vec![
        Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(10)]),
        Row::new(vec![SqlValue::Integer(2), SqlValue::Null]),
        Row::new(vec![SqlValue::Integer(3), SqlValue::Integer(10)]),
        Row::new(vec![SqlValue::Integer(4), SqlValue::Null]),
    ];

    let partitions = partition_rows(rows, &partition_by_column("1"), evaluate_expression).unwrap();

    assert_eq!(partitions.len(), 2, "expected one NULL partition and one Integer(10) partition");
    for partition in &partitions {
        assert_eq!(partition.len(), 2);
        // All rows within a partition share the same key value
        let first_key = partition.rows[0].values[1].clone();
        assert!(partition.rows.iter().all(|r| r.values[1] == first_key));
    }
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
