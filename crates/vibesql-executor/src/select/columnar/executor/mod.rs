//! Native columnar executor - end-to-end columnar query execution
//!
//! This module provides true columnar query execution that operates on `ColumnarBatch`
//! throughout the entire pipeline, avoiding row materialization until final output.

#![allow(clippy::needless_range_loop, clippy::unnecessary_map_or)]
//!
//! ## Architecture
//!
//! ```text
//! Storage → ColumnarBatch → SIMD Filter → SIMD Aggregate → Vec<Row> (only at output)
//!          ↑ Zero-copy     ↑ 4-8x faster  ↑ 10x faster   ↑ Minimal materialization
//! ```
//!
//! ## Key Benefits
//!
//! - **Zero-copy**: ColumnarBatch flows through without row materialization
//! - **SIMD acceleration**: All filtering and aggregation uses vectorized instructions
//! - **Cache efficiency**: Columnar data access patterns are cache-friendly
//! - **Minimal allocations**: Only allocate result rows at the end
//!
//! ## Module Organization
//!
//! - `fused` - Fused filter+aggregate optimization path
//! - `aggregate` - Standard aggregation functions
//! - `expression` - Expression evaluation helpers

mod aggregate;
mod expression;
mod fused;

use aggregate::compute_batch_aggregates;
use fused::{can_use_fused_aggregation, execute_fused_filter_aggregate};
use vibesql_storage::Row;
use vibesql_types::SqlValue;

use super::{
    aggregate::{AggregateOp, AggregateSpec},
    batch::ColumnarBatch,
    filter::ColumnPredicate,
    simd_filter::simd_filter_batch,
};
use crate::{errors::ExecutorError, schema::CombinedSchema};

/// Execute a columnar query end-to-end on a ColumnarBatch
///
/// This is the main entry point for native columnar execution. It accepts
/// a ColumnarBatch from storage and executes filtering and aggregation
/// entirely in columnar format.
///
/// # Arguments
///
/// * `batch` - Input ColumnarBatch from storage layer
/// * `predicates` - Column predicates for SIMD filtering
/// * `aggregates` - Aggregate specifications (SUM, COUNT, etc.)
/// * `schema` - Optional schema for expression evaluation
///
/// # Returns
///
/// A vector of rows containing the aggregated results
pub fn execute_columnar_batch(
    batch: &ColumnarBatch,
    predicates: &[ColumnPredicate],
    aggregates: &[AggregateSpec],
    _schema: Option<&CombinedSchema>,
) -> Result<Vec<Row>, ExecutorError> {
    // Early return for empty input
    if batch.row_count() == 0 {
        let values: Vec<SqlValue> = aggregates
            .iter()
            .map(|spec| match spec.op {
                AggregateOp::Count => SqlValue::Integer(0),
                _ => SqlValue::Null,
            })
            .collect();
        return Ok(vec![Row::new(values)]);
    }

    // Try fused filter+aggregate path first (avoids intermediate batch allocation)
    // This is faster for simple aggregate queries like TPC-H Q6
    if !predicates.is_empty() && can_use_fused_aggregation(aggregates) {
        if let Ok(results) = execute_fused_filter_aggregate(batch, predicates, aggregates) {
            return Ok(vec![Row::new(results)]);
        }
        // Fall through to standard path on failure
    }

    // Standard path: filter first, then aggregate
    // Used when fused path is not applicable or fails

    // Phase 1: Apply auto-vectorized filtering
    #[cfg(feature = "profile-q6")]
    let filter_start = std::time::Instant::now();

    let filtered_batch =
        if predicates.is_empty() { batch.clone() } else { simd_filter_batch(batch, predicates)? };

    #[cfg(feature = "profile-q6")]
    {
        let filter_time = filter_start.elapsed();
        eprintln!(
            "[PROFILE-Q6]   Phase 1 - SIMD Filter: {:?} ({}/{} rows passed)",
            filter_time,
            filtered_batch.row_count(),
            batch.row_count()
        );
    }

    // Phase 2: Compute aggregates on filtered batch
    #[cfg(feature = "profile-q6")]
    let agg_start = std::time::Instant::now();

    let results = compute_batch_aggregates(&filtered_batch, aggregates)?;

    #[cfg(feature = "profile-q6")]
    {
        let agg_time = agg_start.elapsed();
        eprintln!(
            "[PROFILE-Q6]   Phase 2 - SIMD Aggregate: {:?} ({} aggregates)",
            agg_time,
            aggregates.len()
        );
    }

    // Phase 3: Convert to output rows (only materialization point)
    Ok(vec![Row::new(results)])
}

#[cfg(test)]
mod tests {
    use super::{
        super::{aggregate::AggregateSource, batch::ColumnarBatch},
        *,
    };

    fn make_test_batch() -> ColumnarBatch {
        let rows = vec![
            Row::new(vec![SqlValue::Integer(10), SqlValue::Double(1.5)]),
            Row::new(vec![SqlValue::Integer(20), SqlValue::Double(2.5)]),
            Row::new(vec![SqlValue::Integer(30), SqlValue::Double(3.5)]),
            Row::new(vec![SqlValue::Integer(40), SqlValue::Double(4.5)]),
        ];
        ColumnarBatch::from_rows(&rows).unwrap()
    }

    #[test]
    fn test_execute_columnar_batch_sum() {
        let batch = make_test_batch();
        let aggregates =
            vec![AggregateSpec { op: AggregateOp::Sum, source: AggregateSource::Column(0) }];

        let result = execute_columnar_batch(&batch, &[], &aggregates, None).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].get(0), Some(&SqlValue::Integer(100)));
    }

    #[test]
    fn test_execute_columnar_batch_with_filter() {
        let batch = make_test_batch();
        let predicates =
            vec![ColumnPredicate::LessThan { column_idx: 0, value: SqlValue::Integer(25) }];
        let aggregates =
            vec![AggregateSpec { op: AggregateOp::Sum, source: AggregateSource::Column(0) }];

        let result = execute_columnar_batch(&batch, &predicates, &aggregates, None).unwrap();
        assert_eq!(result.len(), 1);
        // Only rows 0 (10) and 1 (20) pass the filter
        assert_eq!(result[0].get(0), Some(&SqlValue::Integer(30)));
    }

    #[test]
    fn test_execute_columnar_batch_multiple_aggregates() {
        let batch = make_test_batch();
        let aggregates = vec![
            AggregateSpec { op: AggregateOp::Sum, source: AggregateSource::Column(0) },
            AggregateSpec { op: AggregateOp::Avg, source: AggregateSource::Column(1) },
            AggregateSpec { op: AggregateOp::Count, source: AggregateSource::CountStar },
        ];

        let result = execute_columnar_batch(&batch, &[], &aggregates, None).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].len(), 3);

        // SUM(col0) = 100
        assert_eq!(result[0].get(0), Some(&SqlValue::Integer(100)));

        // AVG(col1) = 3.0
        if let Some(SqlValue::Double(avg)) = result[0].get(1) {
            assert!((avg - 3.0).abs() < 0.001);
        } else {
            panic!("Expected Double for AVG");
        }

        // COUNT(*) = 4
        assert_eq!(result[0].get(2), Some(&SqlValue::Integer(4)));
    }

    #[test]
    fn test_execute_columnar_batch_empty() {
        let batch = ColumnarBatch::new(2);
        let aggregates = vec![
            AggregateSpec { op: AggregateOp::Sum, source: AggregateSource::Column(0) },
            AggregateSpec { op: AggregateOp::Count, source: AggregateSource::CountStar },
        ];

        let result = execute_columnar_batch(&batch, &[], &aggregates, None).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].get(0), Some(&SqlValue::Null)); // SUM of empty = NULL
        assert_eq!(result[0].get(1), Some(&SqlValue::Integer(0))); // COUNT of empty = 0
    }

    /// Test that aggregation correctly handles NULL values.
    /// This verifies the fix for NULL handling in fused column aggregates.
    #[test]
    fn test_execute_columnar_batch_with_nulls() {
        // Create batch with some NULL values
        let rows = vec![
            Row::new(vec![SqlValue::Integer(10), SqlValue::Double(1.5)]),
            Row::new(vec![SqlValue::Null, SqlValue::Double(2.5)]), // NULL in first column
            Row::new(vec![SqlValue::Integer(30), SqlValue::Null]), // NULL in second column
            Row::new(vec![SqlValue::Integer(40), SqlValue::Double(4.5)]),
        ];
        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        let aggregates = vec![
            AggregateSpec { op: AggregateOp::Sum, source: AggregateSource::Column(0) },
            AggregateSpec { op: AggregateOp::Sum, source: AggregateSource::Column(1) },
            AggregateSpec { op: AggregateOp::Count, source: AggregateSource::CountStar },
        ];

        let result = execute_columnar_batch(&batch, &[], &aggregates, None).unwrap();
        assert_eq!(result.len(), 1);

        // SUM(col0) should be 10 + 30 + 40 = 80 (NULL excluded)
        assert_eq!(result[0].get(0), Some(&SqlValue::Integer(80)));

        // SUM(col1) should be 1.5 + 2.5 + 4.5 = 8.5 (NULL excluded)
        if let Some(SqlValue::Double(sum)) = result[0].get(1) {
            assert!((sum - 8.5).abs() < 0.001);
        } else {
            panic!("Expected Double for SUM(col1)");
        }

        // COUNT(*) = 4 (counts all rows regardless of NULLs)
        assert_eq!(result[0].get(2), Some(&SqlValue::Integer(4)));
    }

    /// Test aggregation with NULLs and filter predicates.
    /// Verifies fused path correctly falls back when NULLs are present.
    #[test]
    fn test_execute_columnar_batch_with_nulls_and_filter() {
        let rows = vec![
            Row::new(vec![SqlValue::Integer(10), SqlValue::Double(1.0)]),
            Row::new(vec![SqlValue::Integer(20), SqlValue::Null]), /* NULL - should be excluded
                                                                    * from sum */
            Row::new(vec![SqlValue::Integer(30), SqlValue::Double(3.0)]),
            Row::new(vec![SqlValue::Integer(40), SqlValue::Double(4.0)]),
        ];
        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        // Filter: col0 < 35 (includes rows 0, 1, 2)
        let predicates =
            vec![ColumnPredicate::LessThan { column_idx: 0, value: SqlValue::Integer(35) }];

        let aggregates =
            vec![AggregateSpec { op: AggregateOp::Sum, source: AggregateSource::Column(1) }];

        let result = execute_columnar_batch(&batch, &predicates, &aggregates, None).unwrap();
        assert_eq!(result.len(), 1);

        // SUM(col1) where col0 < 35: 1.0 + 3.0 = 4.0 (row 1's NULL excluded)
        if let Some(SqlValue::Double(sum)) = result[0].get(0) {
            assert!((sum - 4.0).abs() < 0.001);
        } else {
            panic!("Expected Double for SUM(col1)");
        }
    }

    /// Build a batch with named integer columns "a" and "b".
    fn make_named_batch() -> ColumnarBatch {
        let rows = vec![
            Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(2)]),
            Row::new(vec![SqlValue::Integer(4), SqlValue::Integer(5)]),
            Row::new(vec![SqlValue::Integer(3), SqlValue::Integer(1)]),
        ];
        let mut batch = ColumnarBatch::from_rows(&rows).unwrap();
        batch.set_column_names(vec!["a".to_string(), "b".to_string()]);
        batch
    }

    /// `a + b` as an AST expression over the two named columns.
    fn col_a_plus_col_b() -> vibesql_ast::Expression {
        use vibesql_ast::{BinaryOperator, ColumnIdentifier, Expression};
        Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef(ColumnIdentifier::simple("a", false))),
            op: BinaryOperator::Plus,
            right: Box::new(Expression::ColumnRef(ColumnIdentifier::simple("b", false))),
        }
    }

    /// Regression test for #5672: MIN/MAX over a non-column expression must be
    /// supported (previously rejected with "MIN/MAX not supported for
    /// expression aggregates"). Integer arithmetic must stay integral so the
    /// result matches SQLite (e.g. max(b+c) over integers is an integer).
    #[test]
    fn test_min_max_over_expression() {
        let batch = make_named_batch();
        // a + b yields: 3, 9, 4
        let aggregates = vec![
            AggregateSpec {
                op: AggregateOp::Max,
                source: AggregateSource::Expression(col_a_plus_col_b()),
            },
            AggregateSpec {
                op: AggregateOp::Min,
                source: AggregateSource::Expression(col_a_plus_col_b()),
            },
        ];

        let result = execute_columnar_batch(&batch, &[], &aggregates, None).unwrap();
        assert_eq!(result.len(), 1);

        // MAX(a + b) = 9, preserved as an integer (not 9.0)
        assert_eq!(result[0].get(0), Some(&SqlValue::Integer(9)));
        // MIN(a + b) = 3, preserved as an integer
        assert_eq!(result[0].get(1), Some(&SqlValue::Integer(3)));
    }

    /// MIN/MAX over a float expression returns a Double result.
    #[test]
    fn test_min_max_over_float_expression() {
        use vibesql_ast::{BinaryOperator, ColumnIdentifier, Expression};

        let rows = vec![
            Row::new(vec![SqlValue::Double(1.5), SqlValue::Double(2.5)]),
            Row::new(vec![SqlValue::Double(3.0), SqlValue::Double(0.5)]),
        ];
        let mut batch = ColumnarBatch::from_rows(&rows).unwrap();
        batch.set_column_names(vec!["a".to_string(), "b".to_string()]);

        let expr = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef(ColumnIdentifier::simple("a", false))),
            op: BinaryOperator::Plus,
            right: Box::new(Expression::ColumnRef(ColumnIdentifier::simple("b", false))),
        };

        // a + b yields: 4.0, 3.5
        let aggregates = vec![
            AggregateSpec {
                op: AggregateOp::Max,
                source: AggregateSource::Expression(expr.clone()),
            },
            AggregateSpec { op: AggregateOp::Min, source: AggregateSource::Expression(expr) },
        ];

        let result = execute_columnar_batch(&batch, &[], &aggregates, None).unwrap();
        assert_eq!(result[0].get(0), Some(&SqlValue::Double(4.0)));
        assert_eq!(result[0].get(1), Some(&SqlValue::Double(3.5)));
    }
}
