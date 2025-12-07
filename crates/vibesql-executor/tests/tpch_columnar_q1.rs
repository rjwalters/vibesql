//! TPC-H Q1 columnar execution test
//!
//! Tests that columnar GROUP BY works correctly for TPC-H Q1 style queries.
//! This ensures the columnar_group_by() function can handle real-world GROUP BY aggregations.

use vibesql_executor::select::columnar::{columnar_group_by, AggregateOp};
use vibesql_storage::Row;
use vibesql_types::SqlValue;

#[test]
fn test_tpch_q1_columnar_group_by() {
    // Simplified TPC-H Q1 query:
    // SELECT
    //   l_returnflag,
    //   l_linestatus,
    //   SUM(l_quantity) as sum_qty,
    //   SUM(l_extendedprice) as sum_base_price,
    //   COUNT(*) as count_order
    // FROM lineitem
    // WHERE l_shipdate <= date '1998-12-01'
    // GROUP BY l_returnflag, l_linestatus
    // ORDER BY l_returnflag, l_linestatus

    // Sample lineitem data
    // Columns: l_returnflag, l_linestatus, l_quantity, l_extendedprice, l_shipdate
    let rows = vec![
        Row::new(vec![
            SqlValue::Varchar(arcstr::ArcStr::from("A")),
            SqlValue::Varchar(arcstr::ArcStr::from("F")),
            SqlValue::Double(10.0),
            SqlValue::Double(1000.0),
        ]),
        Row::new(vec![
            SqlValue::Varchar(arcstr::ArcStr::from("N")),
            SqlValue::Varchar(arcstr::ArcStr::from("O")),
            SqlValue::Double(20.0),
            SqlValue::Double(2000.0),
        ]),
        Row::new(vec![
            SqlValue::Varchar(arcstr::ArcStr::from("A")),
            SqlValue::Varchar(arcstr::ArcStr::from("F")),
            SqlValue::Double(15.0),
            SqlValue::Double(1500.0),
        ]),
        Row::new(vec![
            SqlValue::Varchar(arcstr::ArcStr::from("R")),
            SqlValue::Varchar(arcstr::ArcStr::from("F")),
            SqlValue::Double(25.0),
            SqlValue::Double(2500.0),
        ]),
        Row::new(vec![
            SqlValue::Varchar(arcstr::ArcStr::from("A")),
            SqlValue::Varchar(arcstr::ArcStr::from("F")),
            SqlValue::Double(12.0),
            SqlValue::Double(1200.0),
        ]),
        Row::new(vec![
            SqlValue::Varchar(arcstr::ArcStr::from("N")),
            SqlValue::Varchar(arcstr::ArcStr::from("O")),
            SqlValue::Double(30.0),
            SqlValue::Double(3000.0),
        ]),
    ];

    // GROUP BY l_returnflag, l_linestatus
    let group_cols = vec![0, 1];

    // Aggregates: SUM(l_quantity), SUM(l_extendedprice), COUNT(*)
    let agg_cols = vec![
        (2, AggregateOp::Sum),   // SUM(l_quantity)
        (3, AggregateOp::Sum),   // SUM(l_extendedprice)
        (0, AggregateOp::Count), // COUNT(*)
    ];

    let result = columnar_group_by(&rows, &group_cols, &agg_cols, None).unwrap();

    // Should have 3 groups: (A, F), (N, O), (R, F)
    assert_eq!(result.len(), 3);

    // Sort by group keys for deterministic testing
    let mut sorted = result;
    sorted.sort_by(|a: &Row, b: &Row| {
        let a_flag = a.get(0).unwrap();
        let a_status = a.get(1).unwrap();
        let b_flag = b.get(0).unwrap();
        let b_status = b.get(1).unwrap();

        match a_flag.partial_cmp(b_flag).unwrap() {
            std::cmp::Ordering::Equal => a_status.partial_cmp(b_status).unwrap(),
            other => other,
        }
    });

    // Verify group (A, F): 3 rows, sum_qty=37, sum_price=3700
    assert_eq!(sorted[0].get(0), Some(&SqlValue::Varchar(arcstr::ArcStr::from("A"))));
    assert_eq!(sorted[0].get(1), Some(&SqlValue::Varchar(arcstr::ArcStr::from("F"))));
    assert!(matches!(sorted[0].get(2), Some(&SqlValue::Double(qty)) if (qty - 37.0).abs() < 0.001));
    assert!(
        matches!(sorted[0].get(3), Some(&SqlValue::Double(price)) if (price - 3700.0).abs() < 0.001)
    );
    assert_eq!(sorted[0].get(4), Some(&SqlValue::Integer(3)));

    // Verify group (N, O): 2 rows, sum_qty=50, sum_price=5000
    assert_eq!(sorted[1].get(0), Some(&SqlValue::Varchar(arcstr::ArcStr::from("N"))));
    assert_eq!(sorted[1].get(1), Some(&SqlValue::Varchar(arcstr::ArcStr::from("O"))));
    assert!(matches!(sorted[1].get(2), Some(&SqlValue::Double(qty)) if (qty - 50.0).abs() < 0.001));
    assert!(
        matches!(sorted[1].get(3), Some(&SqlValue::Double(price)) if (price - 5000.0).abs() < 0.001)
    );
    assert_eq!(sorted[1].get(4), Some(&SqlValue::Integer(2)));

    // Verify group (R, F): 1 row, sum_qty=25, sum_price=2500
    assert_eq!(sorted[2].get(0), Some(&SqlValue::Varchar(arcstr::ArcStr::from("R"))));
    assert_eq!(sorted[2].get(1), Some(&SqlValue::Varchar(arcstr::ArcStr::from("F"))));
    assert!(matches!(sorted[2].get(2), Some(&SqlValue::Double(qty)) if (qty - 25.0).abs() < 0.001));
    assert!(
        matches!(sorted[2].get(3), Some(&SqlValue::Double(price)) if (price - 2500.0).abs() < 0.001)
    );
    assert_eq!(sorted[2].get(4), Some(&SqlValue::Integer(1)));
}

#[test]
fn test_tpch_q1_with_multiple_aggregates() {
    // More complete TPC-H Q1 with additional aggregates
    // SELECT
    //   l_returnflag,
    //   SUM(l_quantity) as sum_qty,
    //   SUM(l_extendedprice) as sum_base_price,
    //   AVG(l_quantity) as avg_qty,
    //   AVG(l_extendedprice) as avg_price,
    //   COUNT(*) as count_order
    // FROM lineitem
    // GROUP BY l_returnflag

    let rows = vec![
        Row::new(vec![
            SqlValue::Varchar(arcstr::ArcStr::from("A")),
            SqlValue::Double(10.0),
            SqlValue::Double(1000.0),
        ]),
        Row::new(vec![
            SqlValue::Varchar(arcstr::ArcStr::from("N")),
            SqlValue::Double(20.0),
            SqlValue::Double(2000.0),
        ]),
        Row::new(vec![
            SqlValue::Varchar(arcstr::ArcStr::from("A")),
            SqlValue::Double(30.0),
            SqlValue::Double(3000.0),
        ]),
        Row::new(vec![
            SqlValue::Varchar(arcstr::ArcStr::from("R")),
            SqlValue::Double(40.0),
            SqlValue::Double(4000.0),
        ]),
    ];

    // GROUP BY l_returnflag
    let group_cols = vec![0];

    // Multiple aggregates
    let agg_cols = vec![
        (1, AggregateOp::Sum),   // SUM(l_quantity)
        (2, AggregateOp::Sum),   // SUM(l_extendedprice)
        (1, AggregateOp::Avg),   // AVG(l_quantity)
        (2, AggregateOp::Avg),   // AVG(l_extendedprice)
        (0, AggregateOp::Count), // COUNT(*)
    ];

    let result = columnar_group_by(&rows, &group_cols, &agg_cols, None).unwrap();

    // Should have 3 groups
    assert_eq!(result.len(), 3);

    // Find group A
    let a_group = result
        .iter()
        .find(|r: &&Row| matches!(r.get(0), Some(SqlValue::Varchar(s)) if s.as_str() == "A"))
        .unwrap();

    // Verify aggregates for group A
    // SUM(qty) = 40, SUM(price) = 4000, AVG(qty) = 20, AVG(price) = 2000, COUNT = 2
    assert!(matches!(a_group.get(1), Some(&SqlValue::Double(sum)) if (sum - 40.0).abs() < 0.001));
    assert!(matches!(a_group.get(2), Some(&SqlValue::Double(sum)) if (sum - 4000.0).abs() < 0.001));
    assert!(matches!(a_group.get(3), Some(&SqlValue::Double(avg)) if (avg - 20.0).abs() < 0.001));
    assert!(matches!(a_group.get(4), Some(&SqlValue::Double(avg)) if (avg - 2000.0).abs() < 0.001));
    assert_eq!(a_group.get(5), Some(&SqlValue::Integer(2)));
}
