//! Columnar execution for high-performance aggregation queries
//!
//! This module implements column-oriented query execution that avoids
//! materializing full Row objects during table scans, providing 8-10x speedup
//! for aggregation-heavy workloads.
//!
//! ## Architecture
//!
//! Instead of:
//! ```text
//! TableScan → Row{Vec<SqlValue>} → Filter(Row) → Aggregate(Row) → Vec<Row>
//! ```
//!
//! We use:
//! ```text
//! TableScan → ColumnRefs → Filter(native types) → Aggregate → Row
//! ```
//!
//! ## Benefits
//!
//! - **Zero-copy**: Work with `&SqlValue` references instead of cloning
//! - **Cache-friendly**: Access contiguous column data instead of scattered row data
//! - **Type-specialized**: Skip SqlValue enum matching overhead for filters/aggregates
//! - **Minimal allocations**: Only allocate result rows, not intermediate data
//!
//! ## Usage
//!
//! This path is automatically selected for simple aggregate queries that:
//! - Have a single table scan (no JOINs)
//! - Use simple WHERE predicates
//! - Compute aggregates (SUM, COUNT, AVG, MIN, MAX)
//! - Don't use window functions or complex subqueries

// Experimental module - allow dead code warnings for future optimization work
#![allow(dead_code)]

mod batch;
mod scan;
mod filter;
mod aggregate;
mod executor;

// Auto-vectorized SIMD operations - replaces the `wide` crate dependency
// See simd_ops.rs for documentation on why these patterns are structured this way
pub mod simd_ops;

mod simd_aggregate;
mod simd_filter;
mod simd_join;

pub use batch::{ColumnarBatch, ColumnArray};
pub use scan::ColumnarScan;
pub use executor::execute_columnar_batch;
pub use filter::{
    apply_columnar_filter, create_filter_bitmap, create_filter_bitmap_tree,
    evaluate_predicate_tree, extract_column_predicates, extract_predicate_tree, ColumnPredicate,
    PredicateTree,
};
pub use aggregate::{columnar_group_by, compute_multiple_aggregates, extract_aggregates, AggregateOp, AggregateSpec, AggregateSource};

pub use aggregate::compute_aggregates_from_batch;
pub use simd_aggregate::{can_use_simd_for_column, simd_aggregate_f64, simd_aggregate_i64};
pub use simd_filter::simd_filter_batch;
pub use simd_join::columnar_hash_join_inner;

use crate::errors::ExecutorError;
use crate::schema::CombinedSchema;
use vibesql_storage::Row;
use vibesql_types::SqlValue;
use log;

/// Execute a columnar aggregate query with filtering
///
/// This is a simplified entry point for columnar execution that demonstrates
/// the full pipeline: scan → filter → aggregate.
///
/// # Arguments
///
/// * `rows` - Input rows to process
/// * `predicates` - Column predicates for filtering (optional)
/// * `aggregates` - List of (column_index, aggregate_op) pairs to compute
///
/// # Returns
///
/// A single Row containing the computed aggregate values
///
/// # Example
///
/// ```rust,ignore
/// // Compute SUM(col0), AVG(col1) WHERE col2 < 100
/// let predicates = vec![
///     ColumnPredicate::LessThan {
///         column_idx: 2,
///         value: SqlValue::Integer(100),
///     },
/// ];
/// let aggregates = vec![
///     (0, AggregateOp::Sum),
///     (1, AggregateOp::Avg),
/// ];
///
/// let result = execute_columnar_aggregate(&rows, &predicates, &aggregates)?;
/// ```
///
/// Note: This function provides SIMD-accelerated filtering and aggregation through
/// LLVM auto-vectorization of batch-native operations.
pub fn execute_columnar_aggregate(
    rows: &[Row],
    predicates: &[ColumnPredicate],
    aggregates: &[aggregate::AggregateSpec],
    schema: Option<&CombinedSchema>,
) -> Result<Vec<Row>, ExecutorError> {
    // Early return for empty input
    // SQL standard: COUNT returns 0 for empty input, other aggregates return NULL
    if rows.is_empty() {
        let values: Vec<SqlValue> = aggregates
            .iter()
            .map(|spec| match spec.op {
                aggregate::AggregateOp::Count => SqlValue::Integer(0),
                _ => SqlValue::Null,
            })
            .collect();
        return Ok(vec![Row::new(values)]);
    }

    // Phase 1: Convert to columnar batch for SIMD acceleration
    #[cfg(feature = "profile-q6")]
    let batch_start = std::time::Instant::now();

    let batch = ColumnarBatch::from_rows(rows)?;

    #[cfg(feature = "profile-q6")]
    {
        let batch_time = batch_start.elapsed();
        eprintln!("[PROFILE-Q6]   Phase 1 - Convert to batch: {:?}", batch_time);
    }

    // Phase 2: Apply SIMD-accelerated filtering
    #[cfg(feature = "profile-q6")]
    let filter_start = std::time::Instant::now();

    let filtered_batch = if predicates.is_empty() {
        batch.clone()
    } else {
        simd_filter_batch(&batch, predicates)?
    };

    #[cfg(feature = "profile-q6")]
    {
        let filter_time = filter_start.elapsed();
        eprintln!("[PROFILE-Q6]   Phase 2 - SIMD filter: {:?} ({}/{} rows passed)",
            filter_time, filtered_batch.row_count(), rows.len());
    }

    // Phase 3: Compute aggregates directly on batch (no row conversion!)
    #[cfg(feature = "profile-q6")]
    let agg_start = std::time::Instant::now();

    // Use batch-native aggregation to avoid to_rows() conversion overhead
    let results = compute_aggregates_from_batch(&filtered_batch, aggregates, schema)?;

    #[cfg(feature = "profile-q6")]
    {
        let agg_time = agg_start.elapsed();
        eprintln!("[PROFILE-Q6]   Phase 3 - Batch-native aggregate: {:?} ({} aggregates)",
            agg_time, aggregates.len());
    }

    // Return as single row
    Ok(vec![Row::new(results)])
}

/// Execute a query using columnar processing (AST-based interface)
///
/// This is the entry point for columnar execution that accepts AST expressions
/// and converts them to the columnar execution pipeline.
///
/// # Arguments
///
/// * `rows` - The rows to process
/// * `filter` - Optional WHERE clause expression
/// * `aggregates` - SELECT list aggregate expressions
/// * `schema` - Schema for resolving column names to indices
///
/// # Returns
///
/// Some(Result) if the query can be optimized using columnar execution,
/// None if the expressions are too complex for columnar optimization.
///
/// Note: This function uses LLVM auto-vectorization for vectorized execution.
pub fn execute_columnar(
    rows: &[Row],
    filter: Option<&vibesql_ast::Expression>,
    aggregates: &[vibesql_ast::Expression],
    schema: &CombinedSchema,
) -> Option<Result<Vec<Row>, ExecutorError>> {
    log::debug!("  Executing columnar query with {} rows", rows.len());

    // Extract column predicates from filter expression
    let predicates = if let Some(filter_expr) = filter {
        match extract_column_predicates(filter_expr, schema) {
            Some(preds) => {
                log::debug!("    ✓ Extracted {} column predicates for SIMD filtering", preds.len());
                preds
            }
            None => {
                log::debug!("    ✗ Filter too complex for columnar optimization");
                return None; // Too complex for columnar optimization
            }
        }
    } else {
        log::debug!("    No filter predicates");
        vec![] // No filter
    };

    // Extract aggregates from SELECT list
    let agg_specs = match extract_aggregates(aggregates, schema) {
        Some(specs) => {
            log::debug!("    ✓ Extracted {} aggregate operations", specs.len());
            for (i, spec) in specs.iter().enumerate() {
                log::debug!("      Aggregate {}: {:?}", i + 1, spec.op);
            }
            specs
        }
        None => {
            log::debug!("    ✗ Aggregates too complex for columnar optimization");
            return None; // Too complex for columnar optimization
        }
    };

    // Call the simplified interface, passing schema if any aggregates use expressions
    let needs_schema = agg_specs.iter().any(|spec| matches!(spec.source, aggregate::AggregateSource::Expression(_)));
    let schema_ref = if needs_schema { Some(schema) } else { None };

    log::debug!("    Executing SIMD-accelerated columnar aggregation");
    Some(execute_columnar_aggregate(rows, &predicates, &agg_specs, schema_ref))
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_types::Date;

    /// Test the full columnar pipeline: filter + aggregation
    #[test]
    fn test_columnar_pipeline_filtered_sum() {
        // Create test data: TPC-H Q6 style query
        // SELECT SUM(l_extendedprice * l_discount)
        // WHERE l_shipdate >= '1994-01-01'
        //   AND l_shipdate < '1995-01-01'
        //   AND l_discount BETWEEN 0.05 AND 0.07
        //   AND l_quantity < 24

        let rows = vec![
            Row::new(vec![
                SqlValue::Integer(10), // quantity
                SqlValue::Double(100.0), // extendedprice
                SqlValue::Double(0.06), // discount
                SqlValue::Date(Date::new(1994, 6, 1).unwrap()),
            ]),
            Row::new(vec![
                SqlValue::Integer(25), // quantity (filtered out: > 24)
                SqlValue::Double(200.0),
                SqlValue::Double(0.06),
                SqlValue::Date(Date::new(1994, 7, 1).unwrap()),
            ]),
            Row::new(vec![
                SqlValue::Integer(15), // quantity
                SqlValue::Double(150.0),
                SqlValue::Double(0.05), // discount
                SqlValue::Date(Date::new(1994, 8, 1).unwrap()),
            ]),
            Row::new(vec![
                SqlValue::Integer(20), // quantity
                SqlValue::Double(180.0),
                SqlValue::Double(0.08), // discount (filtered out: > 0.07)
                SqlValue::Date(Date::new(1994, 9, 1).unwrap()),
            ]),
        ];

        // Predicates: quantity < 24 AND discount BETWEEN 0.05 AND 0.07
        let predicates = vec![
            ColumnPredicate::LessThan {
                column_idx: 0,
                value: SqlValue::Integer(24),
            },
            ColumnPredicate::Between {
                column_idx: 2,
                low: SqlValue::Double(0.05),
                high: SqlValue::Double(0.07),
            },
        ];

        // Aggregates: SUM(extendedprice), COUNT(*)
        let aggregates = vec![
            AggregateSpec { op: AggregateOp::Sum, source: AggregateSource::Column(1) }, // SUM(extendedprice)
            AggregateSpec { op: AggregateOp::Count, source: AggregateSource::Column(0) }, // COUNT(*)
        ];

        let result = execute_columnar_aggregate(&rows, &predicates, &aggregates, None).unwrap();

        assert_eq!(result.len(), 1);
        let result_row = &result[0];

        // Only rows 0 and 2 pass the filter (quantity < 24 AND discount in range)
        // SUM(extendedprice) = 100.0 + 150.0 = 250.0
        assert!(matches!(result_row.get(0), Some(&SqlValue::Double(sum)) if (sum - 250.0).abs() < 0.001));
        // COUNT(*) = 2
        assert_eq!(result_row.get(1), Some(&SqlValue::Integer(2)));
    }

    /// Test columnar execution with no filtering
    #[test]
    fn test_columnar_pipeline_no_filter() {
        let rows = vec![
            Row::new(vec![SqlValue::Integer(10), SqlValue::Double(1.5)]),
            Row::new(vec![SqlValue::Integer(20), SqlValue::Double(2.5)]),
            Row::new(vec![SqlValue::Integer(30), SqlValue::Double(3.5)]),
        ];

        let predicates = vec![];
        let aggregates = vec![
            AggregateSpec { op: AggregateOp::Sum, source: AggregateSource::Column(0) },
            AggregateSpec { op: AggregateOp::Avg, source: AggregateSource::Column(1) },
            AggregateSpec { op: AggregateOp::Max, source: AggregateSource::Column(0) },
        ];

        let result = execute_columnar_aggregate(&rows, &predicates, &aggregates, None).unwrap();

        assert_eq!(result.len(), 1);
        let result_row = &result[0];

        // SUM(col0) = 60 (preserves integer type per #2545)
        assert_eq!(result_row.get(0), Some(&SqlValue::Integer(60)));
        // AVG(col1) = 2.5
        assert!(matches!(result_row.get(1), Some(&SqlValue::Double(avg)) if (avg - 2.5).abs() < 0.001));
        // MAX(col0) = 30
        assert_eq!(result_row.get(2), Some(&SqlValue::Integer(30)));
    }

    /// Test columnar execution with empty result set
    #[test]
    fn test_columnar_pipeline_empty_result() {
        let rows = vec![
            Row::new(vec![SqlValue::Integer(100)]),
            Row::new(vec![SqlValue::Integer(200)]),
        ];

        // Filter that matches nothing
        let predicates = vec![ColumnPredicate::LessThan {
            column_idx: 0,
            value: SqlValue::Integer(50),
        }];

        let aggregates = vec![
            AggregateSpec { op: AggregateOp::Sum, source: AggregateSource::Column(0) },
            AggregateSpec { op: AggregateOp::Count, source: AggregateSource::Column(0) },
        ];

        let result = execute_columnar_aggregate(&rows, &predicates, &aggregates, None).unwrap();

        assert_eq!(result.len(), 1);
        let result_row = &result[0];

        // SUM of empty set = NULL
        assert_eq!(result_row.get(0), Some(&SqlValue::Null));
        // COUNT of empty set = 0
        assert_eq!(result_row.get(1), Some(&SqlValue::Integer(0)));
    }

    // AST Integration Tests

    use crate::schema::CombinedSchema;
    use vibesql_ast::{BinaryOperator, Expression};
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::DataType;

    fn make_test_schema() -> CombinedSchema {
        let schema = TableSchema::new(
            "test".to_string(),
            vec![
                ColumnSchema::new("quantity".to_string(), DataType::Integer, false),
                ColumnSchema::new("price".to_string(), DataType::DoublePrecision, false),
            ],
        );
        CombinedSchema::from_table("test".to_string(), schema)
    }

    fn make_test_rows_for_ast() -> Vec<Row> {
        vec![
            Row::new(vec![SqlValue::Integer(10), SqlValue::Double(1.5)]),
            Row::new(vec![SqlValue::Integer(20), SqlValue::Double(2.5)]),
            Row::new(vec![SqlValue::Integer(30), SqlValue::Double(3.5)]),
            Row::new(vec![SqlValue::Integer(40), SqlValue::Double(4.5)]),
        ]
    }

    #[test]
    fn test_execute_columnar_simple_aggregate() {
        let rows = make_test_rows_for_ast();
        let schema = make_test_schema();

        // SELECT SUM(price) FROM test
        let aggregates = vec![Expression::AggregateFunction {
            name: "SUM".to_string(),
            distinct: false,
            args: vec![Expression::ColumnRef {
                table: None,
                column: "price".to_string(),
            }],
        }];

        let result = execute_columnar(&rows, None, &aggregates, &schema);
        assert!(result.is_some());

        let rows_result = result.unwrap();
        assert!(rows_result.is_ok());

        let result_rows = rows_result.unwrap();
        assert_eq!(result_rows.len(), 1);
        assert_eq!(result_rows[0].len(), 1);

        // Sum should be 1.5 + 2.5 + 3.5 + 4.5 = 12.0
        if let Some(SqlValue::Double(sum)) = result_rows[0].get(0) {
            assert!((sum - 12.0).abs() < 0.001);
        } else {
            panic!("Expected Double value");
        }
    }

    #[test]
    fn test_execute_columnar_with_filter() {
        let rows = make_test_rows_for_ast();
        let schema = make_test_schema();

        // SELECT SUM(price) FROM test WHERE quantity < 25
        let filter = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef {
                table: None,
                column: "quantity".to_string(),
            }),
            op: BinaryOperator::LessThan,
            right: Box::new(Expression::Literal(SqlValue::Integer(25))),
        };

        let aggregates = vec![Expression::AggregateFunction {
            name: "SUM".to_string(),
            distinct: false,
            args: vec![Expression::ColumnRef {
                table: None,
                column: "price".to_string(),
            }],
        }];

        let result = execute_columnar(&rows, Some(&filter), &aggregates, &schema);
        assert!(result.is_some());

        let rows_result = result.unwrap();
        assert!(rows_result.is_ok());

        let result_rows = rows_result.unwrap();
        assert_eq!(result_rows.len(), 1);
        assert_eq!(result_rows[0].len(), 1);

        // Sum of rows where quantity < 25: 1.5 + 2.5 = 4.0
        if let Some(SqlValue::Double(sum)) = result_rows[0].get(0) {
            assert!((sum - 4.0).abs() < 0.001);
        } else {
            panic!("Expected Double value");
        }
    }

    #[test]
    fn test_execute_columnar_multiple_aggregates() {
        let rows = make_test_rows_for_ast();
        let schema = make_test_schema();

        // SELECT SUM(price), COUNT(*), AVG(quantity) FROM test
        let aggregates = vec![
            Expression::AggregateFunction {
                name: "SUM".to_string(),
                distinct: false,
                args: vec![Expression::ColumnRef {
                    table: None,
                    column: "price".to_string(),
                }],
            },
            Expression::AggregateFunction {
                name: "COUNT".to_string(),
                distinct: false,
                args: vec![Expression::Wildcard],
            },
            Expression::AggregateFunction {
                name: "AVG".to_string(),
                distinct: false,
                args: vec![Expression::ColumnRef {
                    table: None,
                    column: "quantity".to_string(),
                }],
            },
        ];

        let result = execute_columnar(&rows, None, &aggregates, &schema);
        assert!(result.is_some());

        let rows_result = result.unwrap();
        assert!(rows_result.is_ok());

        let result_rows = rows_result.unwrap();
        assert_eq!(result_rows.len(), 1);
        assert_eq!(result_rows[0].len(), 3);

        // Check SUM(price) = 12.0
        if let Some(SqlValue::Double(sum)) = result_rows[0].get(0) {
            assert!((sum - 12.0).abs() < 0.001);
        } else {
            panic!("Expected Double value for SUM");
        }

        // Check COUNT(*) = 4
        assert_eq!(result_rows[0].get(1), Some(&SqlValue::Integer(4)));

        // Check AVG(quantity) = (10+20+30+40)/4 = 25.0
        if let Some(SqlValue::Double(avg)) = result_rows[0].get(2) {
            assert!((avg - 25.0).abs() < 0.001);
        } else {
            panic!("Expected Double value for AVG");
        }
    }

    #[test]
    fn test_execute_columnar_unsupported_distinct() {
        let rows = make_test_rows_for_ast();
        let schema = make_test_schema();

        // SELECT SUM(DISTINCT price) FROM test - should return None
        let aggregates = vec![Expression::AggregateFunction {
            name: "SUM".to_string(),
            distinct: true,
            args: vec![Expression::ColumnRef {
                table: None,
                column: "price".to_string(),
            }],
        }];

        let result = execute_columnar(&rows, None, &aggregates, &schema);
        assert!(result.is_none());
    }

    #[test]
    fn test_execute_columnar_unsupported_complex_filter() {
        let rows = make_test_rows_for_ast();
        let schema = make_test_schema();

        // SELECT SUM(price) FROM test WHERE quantity IN (SELECT ...) - unsupported
        let filter = Expression::ScalarSubquery(Box::new(vibesql_ast::SelectStmt {
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
        }));

        let aggregates = vec![Expression::AggregateFunction {
            name: "SUM".to_string(),
            distinct: false,
            args: vec![Expression::ColumnRef {
                table: None,
                column: "price".to_string(),
            }],
        }];

        let result = execute_columnar(&rows, Some(&filter), &aggregates, &schema);
        assert!(result.is_none());
    }
}
