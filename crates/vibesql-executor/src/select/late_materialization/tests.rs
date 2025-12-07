//! Integration tests for late materialization
//!
//! These tests verify the end-to-end behavior of late materialization
//! for common query patterns.

use std::sync::Arc;
use vibesql_storage::Row;
use vibesql_types::SqlValue;

use super::{gather, lazy_batch, row_ref};
use super::{gather_columns, LazyMaterializedBatch, RowReference, SelectionVector};
use crate::select::columnar::{ColumnArray, ColumnarBatch};
use gather::gather_join_output;

/// Create a sample dataset simulating TPC-H lineitem style data
fn create_tpch_lineitem_sample(row_count: usize) -> ColumnarBatch {
    // Simulated columns: orderkey, partkey, suppkey, quantity, extendedprice, discount, tax
    let orderkeys: Vec<i64> = (0..row_count as i64).collect();
    let partkeys: Vec<i64> = (0..row_count as i64).map(|i| i % 100).collect();
    let suppkeys: Vec<i64> = (0..row_count as i64).map(|i| i % 50).collect();
    let quantities: Vec<f64> = (0..row_count).map(|i| (i % 50) as f64 + 1.0).collect();
    let prices: Vec<f64> = (0..row_count).map(|i| (i % 1000) as f64 * 10.0).collect();
    let discounts: Vec<f64> = (0..row_count).map(|i| (i % 11) as f64 / 100.0).collect();
    let taxes: Vec<f64> = (0..row_count).map(|i| (i % 9) as f64 / 100.0).collect();

    let columns = vec![
        ColumnArray::Int64(Arc::new(orderkeys), None),
        ColumnArray::Int64(Arc::new(partkeys), None),
        ColumnArray::Int64(Arc::new(suppkeys), None),
        ColumnArray::Float64(Arc::new(quantities), None),
        ColumnArray::Float64(Arc::new(prices), None),
        ColumnArray::Float64(Arc::new(discounts), None),
        ColumnArray::Float64(Arc::new(taxes), None),
    ];

    ColumnarBatch::from_columns(
        columns,
        Some(vec![
            "l_orderkey".into(),
            "l_partkey".into(),
            "l_suppkey".into(),
            "l_quantity".into(),
            "l_extendedprice".into(),
            "l_discount".into(),
            "l_tax".into(),
        ]),
    )
    .unwrap()
}

#[test]
fn test_filter_with_late_materialization() {
    // Create 10K rows
    let batch = create_tpch_lineitem_sample(10_000);

    // Simulate filter: quantity > 25 AND discount BETWEEN 0.05 AND 0.07
    // Should select roughly 5% of rows
    let filter_bitmap: Vec<bool> = (0..10_000)
        .map(|i| {
            let qty = (i % 50) as f64 + 1.0;
            let discount = (i % 11) as f64 / 100.0;
            qty > 25.0 && (0.05..=0.07).contains(&discount)
        })
        .collect();

    let selection = SelectionVector::from_bitmap(&filter_bitmap);

    // qty > 25 selects 24/50 = 48% of rows
    // discount in [0.05, 0.07] selects 3/11 = 27% of rows
    // Combined: ~13% = ~1300 rows
    assert!(selection.len() < 2000); // Rough upper bound

    // Create lazy batch with selection
    let lazy_batch = LazyMaterializedBatch::with_selection(
        lazy_batch::SourceData::Columnar(Arc::new(batch)),
        selection.clone(),
    );

    // Only materialize columns needed for output: l_extendedprice (4) and l_discount (5)
    let result = gather_columns(
        match lazy_batch.source() {
            lazy_batch::SourceData::Columnar(b) => b,
            _ => panic!("Expected columnar"),
        },
        lazy_batch.selection(),
        &[4, 5], // extendedprice, discount
    )
    .unwrap();

    // Verify we got the right columns
    assert_eq!(result.len(), selection.len());
    assert!(result.len() < 2000);

    // Each result row should have exactly 2 columns
    for row in &result {
        assert_eq!(row.len(), 2);
    }
}

#[test]
fn test_join_with_late_materialization() {
    // Left: orders table (1000 rows)
    let orders_columns = vec![
        ColumnArray::Int64(Arc::new((0..1000).collect()), None),
        ColumnArray::Int64(Arc::new((0..1000).map(|i| i % 100).collect()), None), // custkey
        ColumnArray::String(Arc::new((0..1000).map(|i| std::sync::Arc::<str>::from(format!("order_{}", i))).collect()), None),
    ];
    let orders = ColumnarBatch::from_columns(
        orders_columns,
        Some(vec!["o_orderkey".into(), "o_custkey".into(), "o_comment".into()]),
    )
    .unwrap();

    // Right: customers table (100 rows)
    let customers_columns = vec![
        ColumnArray::Int64(Arc::new((0..100).collect()), None),
        ColumnArray::String(Arc::new((0..100).map(|i| std::sync::Arc::<str>::from(format!("customer_{}", i))).collect()), None),
    ];
    let customers = ColumnarBatch::from_columns(
        customers_columns,
        Some(vec!["c_custkey".into(), "c_name".into()]),
    )
    .unwrap();

    // Build selection for orders (simulate filter: orderkey < 50)
    let order_filter: Vec<bool> = (0..1000).map(|i| i < 50).collect();
    let order_selection = SelectionVector::from_bitmap(&order_filter);

    // Simulate hash join: for each selected order, find matching customer
    let mut left_indices: Vec<u32> = Vec::new();
    let mut right_indices: Vec<u32> = Vec::new();

    for order_idx in order_selection.iter() {
        let custkey = order_idx % 100; // Same as custkey calculation
        left_indices.push(order_idx);
        right_indices.push(custkey);
    }

    // Late materialization: only gather the columns we need
    // From orders: o_orderkey (0), o_comment (2)
    // From customers: c_name (1)
    let result = gather_join_output(
        &orders,
        &customers,
        &left_indices,
        &right_indices,
        Some(&[0, 2]), // o_orderkey, o_comment
        Some(&[1]),    // c_name
    )
    .unwrap();

    assert_eq!(result.len(), 50); // 50 orders matched

    // Each row should have 3 columns
    for row in &result {
        assert_eq!(row.len(), 3);
        // Verify structure
        assert!(matches!(row.get(0), Some(SqlValue::Integer(_))));
        assert!(matches!(row.get(1), Some(SqlValue::Varchar(_))));
        assert!(matches!(row.get(2), Some(SqlValue::Varchar(_))));
    }
}

#[test]
fn test_multi_filter_chain() {
    let batch = create_tpch_lineitem_sample(5000);

    // First filter: quantity > 10
    let filter1: Vec<bool> = (0..5000).map(|i| (i % 50) as f64 + 1.0 > 10.0).collect();
    let sel1 = SelectionVector::from_bitmap(&filter1);

    // Second filter on sel1 result: discount > 0.05
    // Need to apply relative to sel1
    let mut sel2_indices = Vec::new();
    for idx in sel1.iter() {
        let discount = (idx % 11) as f64 / 100.0;
        if discount > 0.05 {
            sel2_indices.push(idx);
        }
    }
    let sel2 = SelectionVector::from_indices(sel2_indices);

    // Verify chained filtering works
    assert!(sel2.len() < sel1.len());

    // Create lazy batch and materialize
    let lazy = LazyMaterializedBatch::with_selection(
        lazy_batch::SourceData::Columnar(Arc::new(batch)),
        sel2,
    );

    let result = lazy.materialize_column(4).unwrap(); // l_extendedprice
    assert_eq!(result.len(), lazy.len());
}

#[test]
fn test_selection_vector_operations() {
    let a = SelectionVector::from_indices(vec![1, 3, 5, 7, 9]);
    let b = SelectionVector::from_indices(vec![2, 3, 4, 5, 6]);

    // Intersection: 3, 5
    let intersection = a.intersect(&b);
    assert_eq!(intersection.indices(), &[3, 5]);

    // Union: 1, 2, 3, 4, 5, 6, 7, 9
    let union = a.union(&b);
    assert_eq!(union.indices(), &[1, 2, 3, 4, 5, 6, 7, 9]);
}

#[test]
fn test_memory_estimation() {
    // Simulate TPC-H Q6 scenario:
    // - 6M lineitem rows
    // - ~1% pass the filter (60K rows)
    // - Only need 2 columns for the aggregate

    let (early, late, savings) = gather::estimate_savings(
        6_000_000, // total rows
        60_000,    // selected rows
        16,        // total columns in lineitem
        2,         // columns needed for aggregate
        32,        // avg bytes per value
    );

    // Early: 6M × 16 × 32 = 3.07 GB
    assert_eq!(early, 3_072_000_000);

    // Late: 60K × 4 + 60K × 2 × 32 = 240KB + 3.84MB ≈ 4MB
    assert!(late < 5_000_000);

    // Savings should be ~99.8%
    assert!(savings > 0.99);

    println!("TPC-H Q6 Memory Comparison:");
    println!("  Early materialization: {} MB", early / 1_000_000);
    println!("  Late materialization:  {} MB", late / 1_000_000);
    println!("  Savings: {:.1}%", savings * 100.0);
}

#[test]
fn test_row_reference_based_join() {
    // Simulate a join using row references instead of copying data
    let left_rows = vec![
        Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))]),
        Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("Bob"))]),
        Row::new(vec![SqlValue::Integer(3), SqlValue::Varchar(arcstr::ArcStr::from("Carol"))]),
    ];

    let right_rows = vec![
        Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Order-A"))]),
        Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Order-B"))]),
        Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("Order-C"))]),
    ];

    // Instead of copying rows, track references
    let left_source = row_ref::OwnedRowSource::new(0, left_rows);
    let right_source = row_ref::OwnedRowSource::new(1, right_rows);

    // Join result: track which rows matched
    let join_pairs: Vec<(RowReference, RowReference)> = vec![
        (left_source.reference(0), right_source.reference(0)), // Alice - Order-A
        (left_source.reference(0), right_source.reference(1)), // Alice - Order-B
        (left_source.reference(1), right_source.reference(2)), // Bob - Order-C
                                                               // Carol has no orders
    ];

    // Only materialize at output
    let mut output_rows = Vec::new();
    for (left_ref, right_ref) in &join_pairs {
        let left_row = left_source.resolve(left_ref).unwrap();
        let right_row = right_source.resolve(right_ref).unwrap();

        // Combine specific columns
        let values = vec![
            left_row.get(1).unwrap().clone(),  // name
            right_row.get(1).unwrap().clone(), // order description
        ];
        output_rows.push(Row::new(values));
    }

    assert_eq!(output_rows.len(), 3);
    assert_eq!(output_rows[0].get(0), Some(&SqlValue::Varchar(arcstr::ArcStr::from("Alice"))));
    assert_eq!(output_rows[0].get(1), Some(&SqlValue::Varchar(arcstr::ArcStr::from("Order-A"))));
    assert_eq!(output_rows[2].get(0), Some(&SqlValue::Varchar(arcstr::ArcStr::from("Bob"))));
}

#[test]
fn test_contiguous_selection_optimization() {
    // When selection is contiguous, we can use range-based access
    let selection = SelectionVector::from_indices(vec![100, 101, 102, 103, 104]);

    assert!(selection.is_contiguous());
    assert_eq!(selection.as_range(), Some(100..105));

    // Non-contiguous selection
    let sparse = SelectionVector::from_indices(vec![100, 102, 105]);
    assert!(!sparse.is_contiguous());
    assert_eq!(sparse.as_range(), None);
}

#[test]
fn test_selectivity_tracking() {
    let selection = SelectionVector::from_indices(vec![0, 5, 10, 15]);

    // 4 out of 100 = 4%
    let selectivity = selection.selectivity(100);
    assert!((selectivity - 0.04).abs() < 0.001);

    // 4 out of 20 = 20%
    let selectivity = selection.selectivity(20);
    assert!((selectivity - 0.2).abs() < 0.001);
}

#[test]
fn test_lazy_batch_columnar_access() {
    let batch = create_tpch_lineitem_sample(1000);
    let selection = SelectionVector::from_indices(vec![0, 100, 200, 300]);

    let lazy = LazyMaterializedBatch::with_selection(
        lazy_batch::SourceData::Columnar(Arc::new(batch)),
        selection,
    );

    assert!(lazy.is_columnar());

    // Access column array directly for SIMD operations
    let qty_col = lazy.column_array(3).unwrap(); // l_quantity
    assert!(matches!(qty_col, ColumnArray::Float64(_, _)));
}
