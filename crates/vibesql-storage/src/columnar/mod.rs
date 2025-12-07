//! Columnar Storage Format
//!
//! This module provides columnar storage representation for analytical query performance.
//! Unlike row-oriented storage (Vec<Row>), columnar storage groups values by column,
//! enabling SIMD operations and better cache efficiency for analytical workloads.
//!
//! ## Architecture
//!
//! ```text
//! Row-Oriented (current):        Columnar (new):
//! ┌────┬────┬────┐              ┌──────────────┐
//! │id=1│age=25│   │              │ id: [1,2,3]  │
//! ├────┼────┼────┤              ├──────────────┤
//! │id=2│age=30│   │    ──────>  │ age: [25,30,42]
//! ├────┼────┼────┤              ├──────────────┤
//! │id=3│age=42│   │              │ NULL: [F,F,F]│
//! └────┴────┴────┘              └──────────────┘
//! ```
//!
//! ## Performance Benefits
//!
//! - **SIMD**: Process 4-8 values per instruction (vs 1 in row-oriented)
//! - **Cache**: Contiguous memory access (vs jumping between rows)
//! - **Compression**: Column values often have similar patterns
//! - **Projection**: Only load needed columns (vs entire rows)
//!
//! ## Conversion Overhead
//!
//! Conversion between row/columnar has O(n) cost. Use columnar storage for:
//! - Analytical queries (GROUP BY, aggregations, arithmetic)
//! - Large table scans with filtering
//! - Few columns projected (SELECT a, b vs SELECT *)
//!
//! Avoid for:
//! - Point lookups (WHERE id = 123)
//! - Small result sets (<1000 rows)
//! - Many columns projected (SELECT *)
//!
//! ## Module Structure
//!
//! - [`types`]: Column type classification (`ColumnTypeClass`)
//! - [`data`]: Column data storage (`ColumnData`)
//! - [`builder`]: Column construction utilities (`ColumnBuilder`)
//! - [`table`]: Columnar table storage (`ColumnarTable`)

mod builder;
mod data;
mod interner;
mod table;
mod types;

// Public re-exports
pub use data::ColumnData;
pub use interner::{StringInterner, DEFAULT_CARDINALITY_THRESHOLD};
pub use table::ColumnarTable;

#[cfg(test)]
mod tests {
    use vibesql_types::SqlValue;

    use crate::Row;

    use super::*;

    #[test]
    fn test_empty_table() {
        let rows: Vec<Row> = vec![];
        let column_names = vec!["id".to_string(), "name".to_string()];
        let columnar = ColumnarTable::from_rows(&rows, &column_names).unwrap();

        assert_eq!(columnar.row_count(), 0);
        assert_eq!(columnar.column_count(), 2);
    }

    #[test]
    fn test_int64_column() {
        let rows = vec![
            Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(10)]),
            Row::new(vec![SqlValue::Integer(2), SqlValue::Integer(20)]),
            Row::new(vec![SqlValue::Integer(3), SqlValue::Null]),
        ];

        let column_names = vec!["id".to_string(), "value".to_string()];
        let columnar = ColumnarTable::from_rows(&rows, &column_names).unwrap();

        assert_eq!(columnar.row_count(), 3);
        assert_eq!(columnar.column_count(), 2);

        // Check id column
        let id_col = columnar.get_column("id").unwrap();
        assert_eq!(id_col.len(), 3);
        assert!(!id_col.is_null(0));
        assert!(!id_col.is_null(1));
        assert!(!id_col.is_null(2));

        // Check value column
        let value_col = columnar.get_column("value").unwrap();
        assert!(!value_col.is_null(0));
        assert!(!value_col.is_null(1));
        assert!(value_col.is_null(2)); // NULL value
    }

    #[test]
    fn test_float64_column() {
        let rows = vec![
            Row::new(vec![SqlValue::Double(3.14), SqlValue::Float(1.5)]),
            Row::new(vec![SqlValue::Double(2.71), SqlValue::Null]),
        ];

        let column_names = vec!["pi".to_string(), "value".to_string()];
        let columnar = ColumnarTable::from_rows(&rows, &column_names).unwrap();

        assert_eq!(columnar.row_count(), 2);

        let pi_col = columnar.get_column("pi").unwrap();
        assert_eq!(pi_col.len(), 2);
        assert!(!pi_col.is_null(0));
        assert!(!pi_col.is_null(1));

        let value_col = columnar.get_column("value").unwrap();
        assert!(!value_col.is_null(0));
        assert!(value_col.is_null(1));
    }

    #[test]
    fn test_string_column() {
        let rows = vec![
            Row::new(vec![SqlValue::Varchar(arcstr::ArcStr::from("Alice"))]),
            Row::new(vec![SqlValue::Varchar(arcstr::ArcStr::from("Bob"))]),
            Row::new(vec![SqlValue::Null]),
        ];

        let column_names = vec!["name".to_string()];
        let columnar = ColumnarTable::from_rows(&rows, &column_names).unwrap();

        assert_eq!(columnar.row_count(), 3);

        let name_col = columnar.get_column("name").unwrap();
        assert!(!name_col.is_null(0));
        assert!(!name_col.is_null(1));
        assert!(name_col.is_null(2));
    }

    #[test]
    fn test_bool_column() {
        let rows = vec![
            Row::new(vec![SqlValue::Boolean(true)]),
            Row::new(vec![SqlValue::Boolean(false)]),
            Row::new(vec![SqlValue::Null]),
        ];

        let column_names = vec!["flag".to_string()];
        let columnar = ColumnarTable::from_rows(&rows, &column_names).unwrap();

        assert_eq!(columnar.row_count(), 3);

        let flag_col = columnar.get_column("flag").unwrap();
        assert!(!flag_col.is_null(0));
        assert!(!flag_col.is_null(1));
        assert!(flag_col.is_null(2));
    }

    #[test]
    fn test_to_rows_round_trip() {
        let original_rows = vec![
            Row::new(vec![
                SqlValue::Integer(1),
                SqlValue::Double(3.14),
                SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
            ]),
            Row::new(vec![
                SqlValue::Integer(2),
                SqlValue::Null,
                SqlValue::Varchar(arcstr::ArcStr::from("Bob")),
            ]),
            Row::new(vec![SqlValue::Integer(3), SqlValue::Double(2.71), SqlValue::Null]),
        ];

        let column_names = vec!["id".to_string(), "value".to_string(), "name".to_string()];

        // Convert to columnar
        let columnar = ColumnarTable::from_rows(&original_rows, &column_names).unwrap();

        // Convert back to rows
        let reconstructed = columnar.to_rows();

        // Verify round trip
        assert_eq!(reconstructed.len(), original_rows.len());
        for (orig, recon) in original_rows.iter().zip(reconstructed.iter()) {
            assert_eq!(orig.len(), recon.len());
            for i in 0..orig.len() {
                match (orig.get(i), recon.get(i)) {
                    (Some(SqlValue::Integer(a)), Some(SqlValue::Integer(b))) => {
                        assert_eq!(a, b);
                    }
                    (Some(SqlValue::Double(a)), Some(SqlValue::Double(b))) => {
                        assert!((a - b).abs() < 1e-10);
                    }
                    (Some(SqlValue::Varchar(a)), Some(SqlValue::Varchar(b))) => {
                        assert_eq!(a, b);
                    }
                    (Some(SqlValue::Null), Some(SqlValue::Null)) => {}
                    (a, b) => {
                        panic!("Mismatch at column {}: {:?} vs {:?}", i, a, b);
                    }
                }
            }
        }
    }

    #[test]
    fn test_mixed_types_error() {
        let rows = vec![
            Row::new(vec![SqlValue::Integer(1)]),
            Row::new(vec![SqlValue::Double(2.5)]), // Type mismatch
        ];

        let column_names = vec!["value".to_string()];
        let result = ColumnarTable::from_rows(&rows, &column_names);

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("mixed types"));
    }

    #[test]
    fn test_column_count_mismatch() {
        let rows = vec![
            Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(10)]),
            Row::new(vec![SqlValue::Integer(2)]), // Missing column
        ];

        let column_names = vec!["id".to_string(), "value".to_string()];
        let result = ColumnarTable::from_rows(&rows, &column_names);

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("has 1 columns, expected 2"));
    }

    #[test]
    fn test_string_interning_low_cardinality() {
        // Simulate TPC-H l_returnflag column (3 distinct values: A, N, R)
        let rows: Vec<Row> = (0..1000)
            .map(|i| {
                let flag = match i % 3 {
                    0 => "A",
                    1 => "N",
                    _ => "R",
                };
                Row::new(vec![SqlValue::Varchar(arcstr::ArcStr::from(flag))])
            })
            .collect();

        let column_names = vec!["l_returnflag".to_string()];
        let columnar = ColumnarTable::from_rows(&rows, &column_names).unwrap();

        assert_eq!(columnar.row_count(), 1000);

        // Verify string interning: get the underlying data and check Arc pointers
        let col = columnar.get_column("l_returnflag").unwrap();
        if let ColumnData::String { values, nulls: _ } = col {
            // With interning, all "A" values should share the same Arc
            let mut a_arcs: Vec<&std::sync::Arc<str>> = Vec::new();
            let mut n_arcs: Vec<&std::sync::Arc<str>> = Vec::new();
            let mut r_arcs: Vec<&std::sync::Arc<str>> = Vec::new();

            for (i, arc) in values.iter().enumerate() {
                match i % 3 {
                    0 => a_arcs.push(arc),
                    1 => n_arcs.push(arc),
                    _ => r_arcs.push(arc),
                }
            }

            // All "A" values should be pointer-equal (same Arc)
            for i in 1..a_arcs.len() {
                assert!(
                    std::sync::Arc::ptr_eq(a_arcs[0], a_arcs[i]),
                    "String interning failed: 'A' values at positions 0 and {} are different Arcs",
                    i * 3
                );
            }

            // All "N" values should be pointer-equal
            for i in 1..n_arcs.len() {
                assert!(
                    std::sync::Arc::ptr_eq(n_arcs[0], n_arcs[i]),
                    "String interning failed: 'N' values are different Arcs"
                );
            }

            // All "R" values should be pointer-equal
            for i in 1..r_arcs.len() {
                assert!(
                    std::sync::Arc::ptr_eq(r_arcs[0], r_arcs[i]),
                    "String interning failed: 'R' values are different Arcs"
                );
            }

            // Different values should have different Arcs
            assert!(!std::sync::Arc::ptr_eq(a_arcs[0], n_arcs[0]));
            assert!(!std::sync::Arc::ptr_eq(a_arcs[0], r_arcs[0]));
            assert!(!std::sync::Arc::ptr_eq(n_arcs[0], r_arcs[0]));
        } else {
            panic!("Expected String column data");
        }
    }

    #[test]
    fn test_string_interning_values_preserved() {
        // Test that interning preserves the actual string values
        let rows = vec![
            Row::new(vec![SqlValue::Varchar(arcstr::ArcStr::from("active"))]),
            Row::new(vec![SqlValue::Varchar(arcstr::ArcStr::from("pending"))]),
            Row::new(vec![SqlValue::Varchar(arcstr::ArcStr::from("active"))]),
            Row::new(vec![SqlValue::Varchar(arcstr::ArcStr::from("completed"))]),
            Row::new(vec![SqlValue::Varchar(arcstr::ArcStr::from("pending"))]),
        ];

        let column_names = vec!["status".to_string()];
        let columnar = ColumnarTable::from_rows(&rows, &column_names).unwrap();

        // Verify values are correctly stored
        let col = columnar.get_column("status").unwrap();
        assert_eq!(col.get(0), SqlValue::Varchar(arcstr::ArcStr::from("active")));
        assert_eq!(col.get(1), SqlValue::Varchar(arcstr::ArcStr::from("pending")));
        assert_eq!(col.get(2), SqlValue::Varchar(arcstr::ArcStr::from("active")));
        assert_eq!(col.get(3), SqlValue::Varchar(arcstr::ArcStr::from("completed")));
        assert_eq!(col.get(4), SqlValue::Varchar(arcstr::ArcStr::from("pending")));

        // Round trip should preserve values
        let reconstructed = columnar.to_rows();
        assert_eq!(reconstructed.len(), 5);

        if let Some(SqlValue::Varchar(v)) = reconstructed[0].get(0) {
            assert_eq!(v.as_str(), "active");
        } else {
            panic!("Expected Varchar");
        }
    }

    #[test]
    fn test_string_interning_tpch_linestatus() {
        // Simulate TPC-H l_linestatus column (2 distinct values: O, F)
        let rows: Vec<Row> = (0..500)
            .map(|i| {
                let status = if i % 2 == 0 { "O" } else { "F" };
                Row::new(vec![SqlValue::Varchar(arcstr::ArcStr::from(status))])
            })
            .collect();

        let column_names = vec!["l_linestatus".to_string()];
        let columnar = ColumnarTable::from_rows(&rows, &column_names).unwrap();

        let col = columnar.get_column("l_linestatus").unwrap();
        if let ColumnData::String { values, .. } = col {
            // Count unique Arc pointers
            let mut unique_arcs = std::collections::HashSet::new();
            for arc in values.iter() {
                unique_arcs.insert(std::sync::Arc::as_ptr(arc));
            }

            // With interning, there should be exactly 2 unique Arc pointers
            assert_eq!(
                unique_arcs.len(),
                2,
                "Expected 2 unique Arc pointers for 2 distinct values, got {}",
                unique_arcs.len()
            );
        } else {
            panic!("Expected String column data");
        }
    }
}
