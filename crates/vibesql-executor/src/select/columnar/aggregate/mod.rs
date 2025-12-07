//! Columnar aggregation - high-performance aggregate computation
//!
//! This module provides efficient aggregate operations on columnar data,
//! organized into several sub-modules:
//!
//! - [`functions`] - Core aggregate function implementations (SUM, COUNT, AVG, MIN, MAX)
//! - [`expression`] - Expression aggregates (e.g., SUM(a * b))
//! - [`group_by`] - Hash-based GROUP BY aggregation
//!
//! The public API maintains backward compatibility while the implementation
//! is split across focused modules for better maintainability.

mod expression;
mod functions;
mod group_by;

use crate::errors::ExecutorError;
use crate::schema::CombinedSchema;
use vibesql_ast::Expression;
use vibesql_storage::Row;
use vibesql_types::SqlValue;

use super::batch::ColumnarBatch;
use super::scan::ColumnarScan;

// Re-export public types and functions to maintain API compatibility
pub use expression::evaluate_expression_to_column;
pub use expression::evaluate_expression_with_cached_column;
pub use expression::extract_aggregates;
pub use group_by::columnar_group_by;
// SIMD-accelerated GROUP BY for ColumnarBatch - used in native columnar execution path
pub use group_by::columnar_group_by_batch;

/// Aggregate operation type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateOp {
    Sum,
    Count,
    Avg,
    Min,
    Max,
}

/// Source of data for an aggregate - either a simple column or an expression
#[derive(Debug, Clone)]
pub enum AggregateSource {
    /// Simple column reference (fast path) - just a column index
    Column(usize),
    /// Complex expression that needs evaluation (e.g., a * b)
    Expression(Expression),
    /// COUNT(*) - count all rows (not tied to any specific column)
    CountStar,
}

/// A complete aggregate specification
#[derive(Debug, Clone)]
pub struct AggregateSpec {
    pub op: AggregateOp,
    pub source: AggregateSource,
}

/// Compute an aggregate over a column with optional filtering
///
/// This is the core columnar aggregation function that processes
/// columns directly without materializing Row objects.
///
/// Automatically detects when SIMD optimization is available (Int64/Float64 columns)
/// and falls back to scalar implementation for other types.
///
/// # Arguments
///
/// * `scan` - Columnar scan over the data
/// * `column_idx` - Index of the column to aggregate
/// * `op` - Aggregate operation (SUM, COUNT, etc.)
/// * `filter_bitmap` - Optional bitmap of which rows to include
///
/// # Returns
///
/// The aggregated SqlValue
pub fn compute_columnar_aggregate(
    scan: &ColumnarScan,
    column_idx: usize,
    op: AggregateOp,
    filter_bitmap: Option<&[bool]>,
) -> Result<SqlValue, ExecutorError> {
    // Try SIMD path for numeric columns (5-10x speedup via LLVM auto-vectorization)
    {
        use super::simd_aggregate::{
            can_use_simd_for_column, simd_aggregate_f64, simd_aggregate_i64,
        };

        // Detect if column is SIMD-compatible
        if let Some(is_integer) = can_use_simd_for_column(scan, column_idx) {
            // Use SIMD implementation for Int64/Float64 columns
            return if is_integer {
                simd_aggregate_i64(scan, column_idx, op, filter_bitmap)
            } else {
                simd_aggregate_f64(scan, column_idx, op, filter_bitmap)
            };
        }
        // Fall through to scalar path for non-SIMD types
    }

    // Scalar fallback path (always available, used for String, Date, etc.)
    functions::compute_columnar_aggregate_impl(scan, column_idx, op, filter_bitmap)
}

/// Compute multiple aggregates in a single pass over the data
///
/// This is more efficient than computing each aggregate separately
/// as it only scans the data once.
pub fn compute_multiple_aggregates(
    rows: &[Row],
    aggregates: &[AggregateSpec],
    filter_bitmap: Option<&[bool]>,
    schema: Option<&CombinedSchema>,
) -> Result<Vec<SqlValue>, ExecutorError> {
    let scan = ColumnarScan::new(rows);
    let mut results = Vec::with_capacity(aggregates.len());

    for spec in aggregates {
        let result = match &spec.source {
            // Fast path: direct column aggregation
            AggregateSource::Column(column_idx) => {
                compute_columnar_aggregate(&scan, *column_idx, spec.op, filter_bitmap)?
            }
            // Expression path: evaluate expression for each row, then aggregate
            AggregateSource::Expression(expr) => {
                let schema = schema.ok_or_else(|| {
                    ExecutorError::UnsupportedExpression(
                        "Schema required for expression aggregates".to_string(),
                    )
                })?;
                expression::compute_expression_aggregate(
                    rows,
                    expr,
                    spec.op,
                    filter_bitmap,
                    schema,
                )?
            }
            // COUNT(*) path: count all rows
            AggregateSource::CountStar => functions::compute_count(&scan, filter_bitmap)?,
        };
        results.push(result);
    }

    Ok(results)
}

/// Compute aggregates directly from a ColumnarBatch (no row conversion)
///
/// This is a high-performance path that eliminates the overhead of converting
/// ColumnarBatch back to rows before aggregation. It works directly on the
/// typed column arrays for maximum efficiency.
///
/// # Arguments
///
/// * `batch` - The ColumnarBatch to aggregate over (typically filtered)
/// * `aggregates` - List of aggregate specifications to compute
/// * `schema` - Optional schema for expression aggregates
///
/// # Performance
///
/// This function provides 20-30% speedup over the row-based path by:
/// - Avoiding `batch.to_rows()` conversion overhead (~10-15ms for large batches)
/// - Working directly on typed arrays (Int64, Float64) with SIMD operations
/// - Reducing memory allocations
///
/// # Returns
///
/// Vector of aggregate results in the same order as the input aggregates
pub fn compute_aggregates_from_batch(
    batch: &ColumnarBatch,
    aggregates: &[AggregateSpec],
    schema: Option<&CombinedSchema>,
) -> Result<Vec<SqlValue>, ExecutorError> {
    // Handle empty batch
    if batch.row_count() == 0 {
        return Ok(aggregates
            .iter()
            .map(|spec| match spec.op {
                AggregateOp::Count => SqlValue::Integer(0),
                _ => SqlValue::Null,
            })
            .collect());
    }

    let mut results = Vec::with_capacity(aggregates.len());

    for spec in aggregates {
        let result = match &spec.source {
            // Fast path: direct batch aggregation (no row conversion)
            AggregateSource::Column(column_idx) => {
                functions::compute_batch_aggregate(batch, *column_idx, spec.op)?
            }
            // Expression path: SIMD-accelerated evaluation directly on batch columns
            // This eliminates the ~10-15ms overhead of batch.to_rows() for large batches
            AggregateSource::Expression(expr) => {
                let schema = schema.ok_or_else(|| {
                    ExecutorError::UnsupportedExpression(
                        "Schema required for expression aggregates".to_string(),
                    )
                })?;
                // Try batch-native expression evaluation first
                // Falls back to row-based evaluation if column types aren't supported
                match expression::compute_batch_expression_aggregate(batch, expr, spec.op, schema) {
                    Ok(value) => value,
                    Err(ExecutorError::UnsupportedExpression(_)) => {
                        // Fall back to row-based for unsupported column types (Mixed, Date, etc.)
                        let rows = batch.to_rows()?;
                        expression::compute_expression_aggregate(
                            &rows, expr, spec.op, None, schema,
                        )?
                    }
                    Err(other) => return Err(other),
                }
            }
            // COUNT(*) path: just count rows in batch
            AggregateSource::CountStar => SqlValue::Integer(batch.row_count() as i64),
        };
        results.push(result);
    }

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_rows() -> Vec<Row> {
        vec![
            Row::new(vec![SqlValue::Integer(10), SqlValue::Double(1.5)]),
            Row::new(vec![SqlValue::Integer(20), SqlValue::Double(2.5)]),
            Row::new(vec![SqlValue::Integer(30), SqlValue::Double(3.5)]),
        ]
    }

    #[test]
    fn test_sum_aggregate() {
        let rows = make_test_rows();
        let scan = ColumnarScan::new(&rows);

        let result = functions::compute_sum(&scan, 0, None).unwrap();
        assert_eq!(result, SqlValue::Integer(60));

        let result = functions::compute_sum(&scan, 1, None).unwrap();
        assert!(matches!(result, SqlValue::Double(sum) if (sum - 7.5).abs() < 0.001));
    }

    #[test]
    fn test_count_aggregate() {
        let rows = make_test_rows();
        let scan = ColumnarScan::new(&rows);

        let result = functions::compute_count(&scan, None).unwrap();
        assert_eq!(result, SqlValue::Integer(3));
    }

    #[test]
    fn test_sum_with_filter() {
        let rows = make_test_rows();
        let scan = ColumnarScan::new(&rows);
        let filter = vec![true, false, true]; // Include rows 0 and 2

        let result = functions::compute_sum(&scan, 0, Some(&filter)).unwrap();
        assert_eq!(result, SqlValue::Integer(40));
    }

    #[test]
    fn test_multiple_aggregates() {
        let rows = make_test_rows();
        let aggregates = vec![
            AggregateSpec { op: AggregateOp::Sum, source: AggregateSource::Column(0) },
            AggregateSpec { op: AggregateOp::Avg, source: AggregateSource::Column(1) },
        ];

        let results = compute_multiple_aggregates(&rows, &aggregates, None, None).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], SqlValue::Integer(60));
        assert!(matches!(results[1], SqlValue::Double(avg) if (avg - 2.5).abs() < 0.001));
    }

    #[test]
    fn test_extract_aggregates_simple() {
        use crate::schema::CombinedSchema;
        use vibesql_catalog::{ColumnSchema, TableSchema};
        use vibesql_types::DataType;

        // Create a simple schema with two columns
        let schema = TableSchema::new(
            "test".to_string(),
            vec![
                ColumnSchema::new("col1".to_string(), DataType::Integer, false),
                ColumnSchema::new("col2".to_string(), DataType::DoublePrecision, false),
            ],
        );

        let combined_schema = CombinedSchema::from_table("test".to_string(), schema);

        // Test SUM(col1)
        let exprs = vec![Expression::AggregateFunction {
            name: "SUM".to_string(),
            distinct: false,
            args: vec![Expression::ColumnRef { table: None, column: "col1".to_string() }],
        }];

        let result = extract_aggregates(&exprs, &combined_schema);
        assert!(result.is_some());
        let aggregates = result.unwrap();
        assert_eq!(aggregates.len(), 1);
        assert!(matches!(aggregates[0].op, AggregateOp::Sum));
        assert!(matches!(aggregates[0].source, AggregateSource::Column(0)));

        // Test COUNT(*)
        let exprs = vec![Expression::AggregateFunction {
            name: "COUNT".to_string(),
            distinct: false,
            args: vec![Expression::Wildcard],
        }];

        let result = extract_aggregates(&exprs, &combined_schema);
        assert!(result.is_some());
        let aggregates = result.unwrap();
        assert_eq!(aggregates.len(), 1);
        assert!(matches!(aggregates[0].op, AggregateOp::Count));
        assert!(matches!(aggregates[0].source, AggregateSource::CountStar));

        // Test multiple aggregates: SUM(col1), AVG(col2)
        let exprs = vec![
            Expression::AggregateFunction {
                name: "SUM".to_string(),
                distinct: false,
                args: vec![Expression::ColumnRef { table: None, column: "col1".to_string() }],
            },
            Expression::AggregateFunction {
                name: "AVG".to_string(),
                distinct: false,
                args: vec![Expression::ColumnRef { table: None, column: "col2".to_string() }],
            },
        ];

        let result = extract_aggregates(&exprs, &combined_schema);
        assert!(result.is_some());
        let aggregates = result.unwrap();
        assert_eq!(aggregates.len(), 2);
        assert!(matches!(aggregates[0].op, AggregateOp::Sum));
        assert!(matches!(aggregates[0].source, AggregateSource::Column(0)));
        assert!(matches!(aggregates[1].op, AggregateOp::Avg));
        assert!(matches!(aggregates[1].source, AggregateSource::Column(1)));
    }

    #[test]
    fn test_extract_aggregates_unsupported() {
        use crate::schema::CombinedSchema;
        use vibesql_catalog::{ColumnSchema, TableSchema};
        use vibesql_types::DataType;

        let schema = TableSchema::new(
            "test".to_string(),
            vec![ColumnSchema::new("col1".to_string(), DataType::Integer, false)],
        );

        let combined_schema = CombinedSchema::from_table("test".to_string(), schema);

        // Test DISTINCT aggregate (should return None)
        let exprs = vec![Expression::AggregateFunction {
            name: "SUM".to_string(),
            distinct: true,
            args: vec![Expression::ColumnRef { table: None, column: "col1".to_string() }],
        }];

        let result = extract_aggregates(&exprs, &combined_schema);
        assert!(result.is_none());

        // Test non-aggregate expression (should return None)
        let exprs = vec![Expression::ColumnRef { table: None, column: "col1".to_string() }];

        let result = extract_aggregates(&exprs, &combined_schema);
        assert!(result.is_none());

        // Test subquery in aggregate (should return None - not supported)
        let exprs = vec![Expression::AggregateFunction {
            name: "SUM".to_string(),
            distinct: false,
            args: vec![Expression::ScalarSubquery(Box::new(vibesql_ast::SelectStmt {
                with_clause: None,
                distinct: false,
                select_list: vec![],
                into_table: None,
                into_variables: None,
                from: None,
                where_clause: None,
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
                offset: None,
                set_operation: None,
            }))],
        }];

        let result = extract_aggregates(&exprs, &combined_schema);
        assert!(result.is_none());
    }

    #[test]
    fn test_extract_aggregates_with_expression() {
        use crate::schema::CombinedSchema;
        use vibesql_catalog::{ColumnSchema, TableSchema};
        use vibesql_types::DataType;

        // Create a simple schema with two columns
        let schema = TableSchema::new(
            "test".to_string(),
            vec![
                ColumnSchema::new("price".to_string(), DataType::DoublePrecision, false),
                ColumnSchema::new("discount".to_string(), DataType::DoublePrecision, false),
            ],
        );

        let combined_schema = CombinedSchema::from_table("test".to_string(), schema);

        // Test SUM(price * discount) - simple binary operation
        let exprs = vec![Expression::AggregateFunction {
            name: "SUM".to_string(),
            distinct: false,
            args: vec![Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef { table: None, column: "price".to_string() }),
                op: vibesql_ast::BinaryOperator::Multiply,
                right: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "discount".to_string(),
                }),
            }],
        }];

        let result = extract_aggregates(&exprs, &combined_schema);
        assert!(result.is_some());
        let aggregates = result.unwrap();
        assert_eq!(aggregates.len(), 1);
        assert!(matches!(aggregates[0].op, AggregateOp::Sum));
        assert!(matches!(aggregates[0].source, AggregateSource::Expression(_)));
    }

    // GROUP BY tests

    #[test]
    fn test_columnar_group_by_simple() {
        // Test simple GROUP BY with one group column
        // SELECT status, SUM(amount) FROM test GROUP BY status
        let rows = vec![
            Row::new(vec![SqlValue::Varchar(arcstr::ArcStr::from("A")), SqlValue::Double(100.0)]),
            Row::new(vec![SqlValue::Varchar(arcstr::ArcStr::from("B")), SqlValue::Double(200.0)]),
            Row::new(vec![SqlValue::Varchar(arcstr::ArcStr::from("A")), SqlValue::Double(150.0)]),
            Row::new(vec![SqlValue::Varchar(arcstr::ArcStr::from("B")), SqlValue::Double(50.0)]),
        ];

        let group_cols = vec![0]; // Group by status
        let agg_cols = vec![(1, AggregateOp::Sum)]; // SUM(amount)

        let result = columnar_group_by(&rows, &group_cols, &agg_cols, None).unwrap();

        // Should have 2 groups: A and B
        assert_eq!(result.len(), 2);

        // Sort results by group key for deterministic testing
        let mut sorted = result;
        sorted.sort_by(|a, b| {
            let a_key = a.get(0).unwrap();
            let b_key = b.get(0).unwrap();
            a_key.partial_cmp(b_key).unwrap()
        });

        // Check group A: SUM = 250.0
        assert_eq!(sorted[0].get(0), Some(&SqlValue::Varchar(arcstr::ArcStr::from("A"))));
        assert!(
            matches!(sorted[0].get(1), Some(&SqlValue::Double(sum)) if (sum - 250.0).abs() < 0.001)
        );

        // Check group B: SUM = 250.0
        assert_eq!(sorted[1].get(0), Some(&SqlValue::Varchar(arcstr::ArcStr::from("B"))));
        assert!(
            matches!(sorted[1].get(1), Some(&SqlValue::Double(sum)) if (sum - 250.0).abs() < 0.001)
        );
    }

    #[test]
    fn test_columnar_group_by_multiple_group_keys() {
        // Test GROUP BY with multiple columns
        // SELECT status, category, COUNT(*) FROM test GROUP BY status, category
        let rows = vec![
            Row::new(vec![SqlValue::Varchar(arcstr::ArcStr::from("A")), SqlValue::Integer(1)]),
            Row::new(vec![SqlValue::Varchar(arcstr::ArcStr::from("A")), SqlValue::Integer(2)]),
            Row::new(vec![SqlValue::Varchar(arcstr::ArcStr::from("B")), SqlValue::Integer(1)]),
            Row::new(vec![SqlValue::Varchar(arcstr::ArcStr::from("A")), SqlValue::Integer(1)]),
            Row::new(vec![SqlValue::Varchar(arcstr::ArcStr::from("B")), SqlValue::Integer(2)]),
        ];

        let group_cols = vec![0, 1]; // Group by status, category
        let agg_cols = vec![(0, AggregateOp::Count)]; // COUNT(*)

        let result = columnar_group_by(&rows, &group_cols, &agg_cols, None).unwrap();

        // Should have 4 groups: (A,1), (A,2), (B,1), (B,2)
        assert_eq!(result.len(), 4);

        // Verify we have correct counts
        for row in &result {
            let status = row.get(0).unwrap();
            let category = row.get(1).unwrap();
            let count = row.get(2).unwrap();

            match (status, category) {
                (SqlValue::Varchar(s), SqlValue::Integer(1)) if s.as_str() == "A" => {
                    assert_eq!(count, &SqlValue::Integer(2)); // Two rows with A,1
                }
                (SqlValue::Varchar(s), SqlValue::Integer(2)) if s.as_str() == "A" => {
                    assert_eq!(count, &SqlValue::Integer(1)); // One row with A,2
                }
                (SqlValue::Varchar(s), SqlValue::Integer(1)) if s.as_str() == "B" => {
                    assert_eq!(count, &SqlValue::Integer(1)); // One row with B,1
                }
                (SqlValue::Varchar(s), SqlValue::Integer(2)) if s.as_str() == "B" => {
                    assert_eq!(count, &SqlValue::Integer(1)); // One row with B,2
                }
                _ => panic!("Unexpected group key: {:?}, {:?}", status, category),
            }
        }
    }

    #[test]
    fn test_columnar_group_by_multiple_aggregates() {
        // Test GROUP BY with multiple aggregate functions
        // SELECT category, SUM(price), AVG(quantity), COUNT(*) FROM test GROUP BY category
        let rows = vec![
            Row::new(vec![SqlValue::Integer(1), SqlValue::Double(100.0), SqlValue::Integer(10)]),
            Row::new(vec![SqlValue::Integer(2), SqlValue::Double(200.0), SqlValue::Integer(20)]),
            Row::new(vec![SqlValue::Integer(1), SqlValue::Double(150.0), SqlValue::Integer(15)]),
        ];

        let group_cols = vec![0]; // Group by category
        let agg_cols = vec![
            (1, AggregateOp::Sum),   // SUM(price)
            (2, AggregateOp::Avg),   // AVG(quantity)
            (0, AggregateOp::Count), // COUNT(*)
        ];

        let result = columnar_group_by(&rows, &group_cols, &agg_cols, None).unwrap();

        // Should have 2 groups
        assert_eq!(result.len(), 2);

        // Sort by category for deterministic testing
        let mut sorted = result;
        sorted.sort_by(|a, b| {
            let a_key = a.get(0).unwrap();
            let b_key = b.get(0).unwrap();
            a_key.partial_cmp(b_key).unwrap()
        });

        // Group 1: SUM=250, AVG=12.5, COUNT=2
        assert_eq!(sorted[0].get(0), Some(&SqlValue::Integer(1)));
        assert!(
            matches!(sorted[0].get(1), Some(&SqlValue::Double(sum)) if (sum - 250.0).abs() < 0.001)
        );
        assert!(
            matches!(sorted[0].get(2), Some(&SqlValue::Double(avg)) if (avg - 12.5).abs() < 0.001)
        );
        assert_eq!(sorted[0].get(3), Some(&SqlValue::Integer(2)));

        // Group 2: SUM=200, AVG=20.0, COUNT=1
        assert_eq!(sorted[1].get(0), Some(&SqlValue::Integer(2)));
        assert!(
            matches!(sorted[1].get(1), Some(&SqlValue::Double(sum)) if (sum - 200.0).abs() < 0.001)
        );
        assert!(
            matches!(sorted[1].get(2), Some(&SqlValue::Double(avg)) if (avg - 20.0).abs() < 0.001)
        );
        assert_eq!(sorted[1].get(3), Some(&SqlValue::Integer(1)));
    }

    #[test]
    fn test_columnar_group_by_with_filter() {
        // Test GROUP BY with pre-filtering
        // SELECT status, SUM(amount) FROM test WHERE amount > 100 GROUP BY status
        let rows = vec![
            Row::new(vec![SqlValue::Varchar(arcstr::ArcStr::from("A")), SqlValue::Double(100.0)]),
            Row::new(vec![SqlValue::Varchar(arcstr::ArcStr::from("B")), SqlValue::Double(200.0)]),
            Row::new(vec![SqlValue::Varchar(arcstr::ArcStr::from("A")), SqlValue::Double(150.0)]),
            Row::new(vec![SqlValue::Varchar(arcstr::ArcStr::from("B")), SqlValue::Double(50.0)]),
        ];

        // Filter: amount > 100 (rows 1 and 2)
        let filter = vec![false, true, true, false];

        let group_cols = vec![0]; // Group by status
        let agg_cols = vec![(1, AggregateOp::Sum)]; // SUM(amount)

        let result = columnar_group_by(&rows, &group_cols, &agg_cols, Some(&filter)).unwrap();

        // Should have 2 groups (only rows passing filter)
        assert_eq!(result.len(), 2);

        // Sort results by group key
        let mut sorted = result;
        sorted.sort_by(|a, b| {
            let a_key = a.get(0).unwrap();
            let b_key = b.get(0).unwrap();
            a_key.partial_cmp(b_key).unwrap()
        });

        // Check group A: only row 2 (150.0) passes filter
        assert_eq!(sorted[0].get(0), Some(&SqlValue::Varchar(arcstr::ArcStr::from("A"))));
        assert!(
            matches!(sorted[0].get(1), Some(&SqlValue::Double(sum)) if (sum - 150.0).abs() < 0.001)
        );

        // Check group B: only row 1 (200.0) passes filter
        assert_eq!(sorted[1].get(0), Some(&SqlValue::Varchar(arcstr::ArcStr::from("B"))));
        assert!(
            matches!(sorted[1].get(1), Some(&SqlValue::Double(sum)) if (sum - 200.0).abs() < 0.001)
        );
    }

    #[test]
    fn test_columnar_group_by_empty_input() {
        let rows: Vec<Row> = vec![];
        let group_cols = vec![0];
        let agg_cols = vec![(1, AggregateOp::Sum)];

        let result = columnar_group_by(&rows, &group_cols, &agg_cols, None).unwrap();

        // Should return empty result for empty input
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_columnar_group_by_null_in_group_key() {
        // Test that NULL values in group keys are handled correctly
        let rows = vec![
            Row::new(vec![SqlValue::Varchar(arcstr::ArcStr::from("A")), SqlValue::Double(100.0)]),
            Row::new(vec![SqlValue::Null, SqlValue::Double(200.0)]),
            Row::new(vec![SqlValue::Varchar(arcstr::ArcStr::from("A")), SqlValue::Double(150.0)]),
            Row::new(vec![SqlValue::Null, SqlValue::Double(50.0)]),
        ];

        let group_cols = vec![0]; // Group by first column
        let agg_cols = vec![(1, AggregateOp::Sum)]; // SUM

        let result = columnar_group_by(&rows, &group_cols, &agg_cols, None).unwrap();

        // Should have 2 groups: "A" and NULL
        assert_eq!(result.len(), 2);

        // Find the groups
        let a_group =
            result.iter().find(|r| matches!(r.get(0), Some(SqlValue::Varchar(s)) if s.as_str() == "A"));
        let null_group = result.iter().find(|r| matches!(r.get(0), Some(SqlValue::Null)));

        assert!(a_group.is_some());
        assert!(null_group.is_some());

        // Check "A" group: 100 + 150 = 250
        assert!(
            matches!(a_group.unwrap().get(1), Some(&SqlValue::Double(sum)) if (sum - 250.0).abs() < 0.001)
        );

        // Check NULL group: 200 + 50 = 250
        assert!(
            matches!(null_group.unwrap().get(1), Some(&SqlValue::Double(sum)) if (sum - 250.0).abs() < 0.001)
        );
    }
}
