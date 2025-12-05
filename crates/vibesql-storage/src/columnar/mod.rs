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
mod table;
mod types;

// Public re-exports
pub use data::ColumnData;
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
            Row::new(vec![SqlValue::Varchar("Alice".to_string())]),
            Row::new(vec![SqlValue::Varchar("Bob".to_string())]),
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
                SqlValue::Varchar("Alice".to_string()),
            ]),
            Row::new(vec![
                SqlValue::Integer(2),
                SqlValue::Null,
                SqlValue::Varchar("Bob".to_string()),
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
}
