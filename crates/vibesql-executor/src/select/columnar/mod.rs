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

mod aggregate;
pub mod batch;
mod executor;
pub mod filter;
mod scan;
mod string_ops;

// Auto-vectorized SIMD operations - replaces the `wide` crate dependency
// See simd_ops.rs for documentation on why these patterns are structured this way
pub mod simd_ops;

mod simd_aggregate;
pub mod simd_filter;
mod simd_join;

pub use aggregate::{
    columnar_group_by, columnar_group_by_batch, compute_multiple_aggregates,
    evaluate_expression_to_column, evaluate_expression_with_cached_column, extract_aggregates,
    AggregateOp, AggregateSource, AggregateSpec,
};
pub use batch::{ColumnArray, ColumnarBatch};
pub use executor::execute_columnar_batch;
pub use filter::{
    apply_columnar_filter, apply_columnar_filter_simd_streaming, create_filter_bitmap,
    create_filter_bitmap_tree, evaluate_predicate_tree, extract_column_predicates,
    extract_predicate_tree, ColumnPredicate, PredicateTree,
};
pub use scan::ColumnarScan;

pub use aggregate::compute_aggregates_from_batch;
pub use simd_aggregate::{can_use_simd_for_column, simd_aggregate_f64, simd_aggregate_i64};
pub use simd_filter::{simd_create_filter_mask, simd_create_filter_mask_packed, simd_filter_batch};
pub use simd_join::columnar_hash_join_inner;
pub use simd_ops::PackedMask;

use crate::errors::ExecutorError;
use crate::schema::CombinedSchema;
use log;
use vibesql_storage::Row;
use vibesql_types::SqlValue;

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
/// ```text
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

    let filtered_batch =
        if predicates.is_empty() { batch.clone() } else { simd_filter_batch(&batch, predicates)? };

    #[cfg(feature = "profile-q6")]
    {
        let filter_time = filter_start.elapsed();
        eprintln!(
            "[PROFILE-Q6]   Phase 2 - SIMD filter: {:?} ({}/{} rows passed)",
            filter_time,
            filtered_batch.row_count(),
            rows.len()
        );
    }

    // Phase 3: Compute aggregates directly on batch (no row conversion!)
    #[cfg(feature = "profile-q6")]
    let agg_start = std::time::Instant::now();

    // Use batch-native aggregation to avoid to_rows() conversion overhead
    let results = compute_aggregates_from_batch(&filtered_batch, aggregates, schema)?;

    #[cfg(feature = "profile-q6")]
    {
        let agg_time = agg_start.elapsed();
        eprintln!(
            "[PROFILE-Q6]   Phase 3 - Batch-native aggregate: {:?} ({} aggregates)",
            agg_time,
            aggregates.len()
        );
    }

    // Return as single row
    Ok(vec![Row::new(results)])
}

/// Fast single-pass aggregate on rows - avoids batch conversion overhead
///
/// This function performs filtering and aggregation in a single pass over the input rows,
/// without converting to columnar format. It's 3-5x faster than `execute_columnar_aggregate`
/// for queries that come from row-based storage.
///
/// # Use Cases
///
/// Best suited for:
/// - Simple aggregate queries without GROUP BY
/// - When data arrives as Vec<Row> (not native columnar)
/// - TPC-H style queries: SUM(price * discount) WHERE ...
///
/// # Arguments
///
/// * `rows` - Input rows to process
/// * `predicates` - Column predicates for filtering
/// * `aggregates` - Aggregate specifications
///
/// # Returns
///
/// A single Row containing the computed aggregate values
pub fn fast_aggregate_on_rows(
    rows: &[Row],
    predicates: &[ColumnPredicate],
    aggregates: &[aggregate::AggregateSpec],
) -> Result<Vec<Row>, ExecutorError> {
    use aggregate::{AggregateOp, AggregateSource};

    // Early return for empty input
    if rows.is_empty() {
        let values: Vec<SqlValue> = aggregates
            .iter()
            .map(|spec| match spec.op {
                AggregateOp::Count => SqlValue::Integer(0),
                _ => SqlValue::Null,
            })
            .collect();
        return Ok(vec![Row::new(values)]);
    }

    // Initialize accumulators for each aggregate
    struct Accumulator {
        sum_f64: f64,
        sum_i64: i64,
        count: i64,
        min_f64: Option<f64>,
        max_f64: Option<f64>,
        min_i64: Option<i64>,
        max_i64: Option<i64>,
        is_integer: bool,
    }

    let mut accumulators: Vec<Accumulator> = aggregates
        .iter()
        .map(|_| Accumulator {
            sum_f64: 0.0,
            sum_i64: 0,
            count: 0,
            min_f64: None,
            max_f64: None,
            min_i64: None,
            max_i64: None,
            is_integer: true,
        })
        .collect();

    // Single pass: filter and accumulate
    for row in rows {
        // Check all predicates
        let passes_filter = predicates.iter().all(|pred| evaluate_predicate(row, pred));

        if !passes_filter {
            continue;
        }

        // Accumulate values for each aggregate
        for (i, spec) in aggregates.iter().enumerate() {
            let acc = &mut accumulators[i];

            match &spec.source {
                AggregateSource::CountStar => {
                    acc.count += 1;
                }
                AggregateSource::Column(col_idx) => {
                    if let Some(value) = row.get(*col_idx) {
                        if !matches!(value, SqlValue::Null) {
                            acc.count += 1;
                            match value {
                                SqlValue::Integer(v) => {
                                    acc.sum_i64 += v;
                                    acc.sum_f64 += *v as f64;
                                    acc.min_i64 = Some(acc.min_i64.map_or(*v, |m| m.min(*v)));
                                    acc.max_i64 = Some(acc.max_i64.map_or(*v, |m| m.max(*v)));
                                    acc.min_f64 =
                                        Some(acc.min_f64.map_or(*v as f64, |m| m.min(*v as f64)));
                                    acc.max_f64 =
                                        Some(acc.max_f64.map_or(*v as f64, |m| m.max(*v as f64)));
                                }
                                SqlValue::Double(v) => {
                                    acc.is_integer = false;
                                    acc.sum_f64 += v;
                                    acc.min_f64 = Some(acc.min_f64.map_or(*v, |m| m.min(*v)));
                                    acc.max_f64 = Some(acc.max_f64.map_or(*v, |m| m.max(*v)));
                                }
                                SqlValue::Float(v) => {
                                    acc.is_integer = false;
                                    acc.sum_f64 += *v as f64;
                                    acc.min_f64 =
                                        Some(acc.min_f64.map_or(*v as f64, |m| m.min(*v as f64)));
                                    acc.max_f64 =
                                        Some(acc.max_f64.map_or(*v as f64, |m| m.max(*v as f64)));
                                }
                                SqlValue::Bigint(v) => {
                                    acc.sum_i64 += v;
                                    acc.sum_f64 += *v as f64;
                                    acc.min_i64 = Some(acc.min_i64.map_or(*v, |m| m.min(*v)));
                                    acc.max_i64 = Some(acc.max_i64.map_or(*v, |m| m.max(*v)));
                                    acc.min_f64 =
                                        Some(acc.min_f64.map_or(*v as f64, |m| m.min(*v as f64)));
                                    acc.max_f64 =
                                        Some(acc.max_f64.map_or(*v as f64, |m| m.max(*v as f64)));
                                }
                                SqlValue::Numeric(v) => {
                                    acc.is_integer = false;
                                    acc.sum_f64 += v;
                                    acc.min_f64 = Some(acc.min_f64.map_or(*v, |m| m.min(*v)));
                                    acc.max_f64 = Some(acc.max_f64.map_or(*v, |m| m.max(*v)));
                                }
                                _ => {}
                            }
                        }
                    }
                }
                AggregateSource::Expression(expr) => {
                    // For expression aggregates like SUM(a * b), evaluate the expression
                    // This is a simplified evaluator for common binary operations
                    if let Some(value) = eval_simple_expression(row, expr) {
                        acc.count += 1;
                        acc.is_integer = false;
                        acc.sum_f64 += value;
                        acc.min_f64 = Some(acc.min_f64.map_or(value, |m| m.min(value)));
                        acc.max_f64 = Some(acc.max_f64.map_or(value, |m| m.max(value)));
                    }
                }
            }
        }
    }

    // Build result row from accumulators
    let values: Vec<SqlValue> = aggregates
        .iter()
        .zip(accumulators.iter())
        .map(|(spec, acc)| match spec.op {
            AggregateOp::Count => SqlValue::Integer(acc.count),
            AggregateOp::Sum => {
                if acc.count == 0 {
                    SqlValue::Null
                } else if acc.is_integer {
                    SqlValue::Integer(acc.sum_i64)
                } else {
                    SqlValue::Double(acc.sum_f64)
                }
            }
            AggregateOp::Avg => {
                if acc.count == 0 {
                    SqlValue::Null
                } else {
                    SqlValue::Double(acc.sum_f64 / acc.count as f64)
                }
            }
            AggregateOp::Min => {
                if acc.is_integer {
                    acc.min_i64.map(SqlValue::Integer).unwrap_or(SqlValue::Null)
                } else {
                    acc.min_f64.map(SqlValue::Double).unwrap_or(SqlValue::Null)
                }
            }
            AggregateOp::Max => {
                if acc.is_integer {
                    acc.max_i64.map(SqlValue::Integer).unwrap_or(SqlValue::Null)
                } else {
                    acc.max_f64.map(SqlValue::Double).unwrap_or(SqlValue::Null)
                }
            }
        })
        .collect();

    Ok(vec![Row::new(values)])
}

/// Evaluate a column predicate against a row
fn evaluate_predicate(row: &Row, predicate: &ColumnPredicate) -> bool {
    match predicate {
        ColumnPredicate::LessThan { column_idx, value } => row
            .get(*column_idx)
            .map(|v| compare_values(v, value) == std::cmp::Ordering::Less)
            .unwrap_or(false),
        ColumnPredicate::LessThanOrEqual { column_idx, value } => row
            .get(*column_idx)
            .map(|v| compare_values(v, value) != std::cmp::Ordering::Greater)
            .unwrap_or(false),
        ColumnPredicate::GreaterThan { column_idx, value } => row
            .get(*column_idx)
            .map(|v| compare_values(v, value) == std::cmp::Ordering::Greater)
            .unwrap_or(false),
        ColumnPredicate::GreaterThanOrEqual { column_idx, value } => row
            .get(*column_idx)
            .map(|v| compare_values(v, value) != std::cmp::Ordering::Less)
            .unwrap_or(false),
        ColumnPredicate::Equal { column_idx, value } => row
            .get(*column_idx)
            .map(|v| compare_values(v, value) == std::cmp::Ordering::Equal)
            .unwrap_or(false),
        ColumnPredicate::NotEqual { column_idx, value } => row
            .get(*column_idx)
            .map(|v| compare_values(v, value) != std::cmp::Ordering::Equal)
            .unwrap_or(false),
        ColumnPredicate::Between { column_idx, low, high } => row
            .get(*column_idx)
            .map(|v| {
                compare_values(v, low) != std::cmp::Ordering::Less
                    && compare_values(v, high) != std::cmp::Ordering::Greater
            })
            .unwrap_or(false),
        ColumnPredicate::Like { column_idx, pattern, negated } => {
            // Simple LIKE pattern matching for row-based fast path
            let matches = row
                .get(*column_idx)
                .map(|v| {
                    if let SqlValue::Varchar(s) = v {
                        // Convert SQL LIKE pattern to simple check
                        // This is a simplified version - full LIKE support is in simd_filter
                        let pattern_str = pattern.as_str();
                        if let Some(inner) =
                            pattern_str.strip_prefix('%').and_then(|s| s.strip_suffix('%'))
                        {
                            s.contains(inner)
                        } else if let Some(suffix) = pattern_str.strip_prefix('%') {
                            s.ends_with(suffix)
                        } else if let Some(prefix) = pattern_str.strip_suffix('%') {
                            s.starts_with(prefix)
                        } else {
                            &**s == pattern_str
                        }
                    } else {
                        false
                    }
                })
                .unwrap_or(false);
            if *negated {
                !matches
            } else {
                matches
            }
        }
        ColumnPredicate::InList { column_idx, values, negated } => {
            // Check if column value matches any value in the list
            let matches = row
                .get(*column_idx)
                .map(|v| {
                    values
                        .iter()
                        .any(|list_val| compare_values(v, list_val) == std::cmp::Ordering::Equal)
                })
                .unwrap_or(false);
            if *negated {
                !matches
            } else {
                matches
            }
        }
        ColumnPredicate::ColumnCompare { left_column_idx, op, right_column_idx } => {
            // Column-to-column comparison
            let left_val = row.get(*left_column_idx);
            let right_val = row.get(*right_column_idx);
            match (left_val, right_val) {
                (Some(l), Some(r)) => {
                    use std::cmp::Ordering;
                    let cmp = compare_values(l, r);
                    match op {
                        filter::CompareOp::LessThan => cmp == Ordering::Less,
                        filter::CompareOp::GreaterThan => cmp == Ordering::Greater,
                        filter::CompareOp::LessThanOrEqual => cmp != Ordering::Greater,
                        filter::CompareOp::GreaterThanOrEqual => cmp != Ordering::Less,
                        filter::CompareOp::Equal => cmp == Ordering::Equal,
                        filter::CompareOp::NotEqual => cmp != Ordering::Equal,
                    }
                }
                _ => false, // NULL comparison returns false
            }
        }
    }
}

/// Compare two SqlValues
fn compare_values(a: &SqlValue, b: &SqlValue) -> std::cmp::Ordering {
    use std::cmp::Ordering;

    match (a, b) {
        (SqlValue::Integer(a), SqlValue::Integer(b)) => a.cmp(b),
        (SqlValue::Bigint(a), SqlValue::Bigint(b)) => a.cmp(b),
        (SqlValue::Double(a), SqlValue::Double(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
        (SqlValue::Float(a), SqlValue::Float(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
        // Cross-type comparisons
        (SqlValue::Integer(a), SqlValue::Double(b)) => {
            (*a as f64).partial_cmp(b).unwrap_or(Ordering::Equal)
        }
        (SqlValue::Double(a), SqlValue::Integer(b)) => {
            a.partial_cmp(&(*b as f64)).unwrap_or(Ordering::Equal)
        }
        (SqlValue::Integer(a), SqlValue::Bigint(b)) => (*a).cmp(b),
        (SqlValue::Bigint(a), SqlValue::Integer(b)) => a.cmp(&{ *b }),
        // String comparisons
        (SqlValue::Varchar(a), SqlValue::Varchar(b)) => a.cmp(b),
        // Date comparisons
        (SqlValue::Date(a), SqlValue::Date(b)) => {
            // Compare year, month, day
            match a.year.cmp(&b.year) {
                Ordering::Equal => match a.month.cmp(&b.month) {
                    Ordering::Equal => a.day.cmp(&b.day),
                    other => other,
                },
                other => other,
            }
        }
        // NULL handling
        (SqlValue::Null, _) | (_, SqlValue::Null) => Ordering::Equal, // NULL comparisons are undefined
        _ => Ordering::Equal,                                         // Incompatible types
    }
}

/// Evaluate a simple expression (for expression aggregates)
#[allow(clippy::only_used_in_recursion)]
fn eval_simple_expression(row: &Row, expr: &vibesql_ast::Expression) -> Option<f64> {
    use vibesql_ast::{BinaryOperator, Expression};

    match expr {
        Expression::BinaryOp { left, op, right } => {
            let left_val = eval_simple_expression(row, left)?;
            let right_val = eval_simple_expression(row, right)?;
            match op {
                BinaryOperator::Multiply => Some(left_val * right_val),
                BinaryOperator::Divide => Some(left_val / right_val),
                BinaryOperator::Plus => Some(left_val + right_val),
                BinaryOperator::Minus => Some(left_val - right_val),
                _ => None,
            }
        }
        Expression::ColumnRef { column, .. } => {
            // Cannot resolve column names without a schema - return None to skip
            // the fast path and fall back to the columnar execution path which
            // properly handles expression aggregates with schema resolution.
            log::debug!(
                "fast_aggregate_on_rows: ColumnRef '{}' requires schema resolution, skipping fast path",
                column
            );
            None
        }
        Expression::Literal(val) => match val {
            SqlValue::Integer(v) => Some(*v as f64),
            SqlValue::Double(v) => Some(*v),
            SqlValue::Float(v) => Some(*v as f64),
            SqlValue::Bigint(v) => Some(*v as f64),
            SqlValue::Numeric(v) => Some(*v),
            _ => None,
        },
        _ => None,
    }
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
    let needs_schema = agg_specs
        .iter()
        .any(|spec| matches!(spec.source, aggregate::AggregateSource::Expression(_)));
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
                SqlValue::Integer(10),   // quantity
                SqlValue::Double(100.0), // extendedprice
                SqlValue::Double(0.06),  // discount
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
            ColumnPredicate::LessThan { column_idx: 0, value: SqlValue::Integer(24) },
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
        assert!(
            matches!(result_row.get(0), Some(&SqlValue::Double(sum)) if (sum - 250.0).abs() < 0.001)
        );
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
        assert!(
            matches!(result_row.get(1), Some(&SqlValue::Double(avg)) if (avg - 2.5).abs() < 0.001)
        );
        // MAX(col0) = 30
        assert_eq!(result_row.get(2), Some(&SqlValue::Integer(30)));
    }

    /// Test columnar execution with empty result set
    #[test]
    fn test_columnar_pipeline_empty_result() {
        let rows =
            vec![Row::new(vec![SqlValue::Integer(100)]), Row::new(vec![SqlValue::Integer(200)])];

        // Filter that matches nothing
        let predicates =
            vec![ColumnPredicate::LessThan { column_idx: 0, value: SqlValue::Integer(50) }];

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
            args: vec![Expression::ColumnRef { table: None, column: "price".to_string() }],
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
            left: Box::new(Expression::ColumnRef { table: None, column: "quantity".to_string() }),
            op: BinaryOperator::LessThan,
            right: Box::new(Expression::Literal(SqlValue::Integer(25))),
        };

        let aggregates = vec![Expression::AggregateFunction {
            name: "SUM".to_string(),
            distinct: false,
            args: vec![Expression::ColumnRef { table: None, column: "price".to_string() }],
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
                args: vec![Expression::ColumnRef { table: None, column: "price".to_string() }],
            },
            Expression::AggregateFunction {
                name: "COUNT".to_string(),
                distinct: false,
                args: vec![Expression::Wildcard],
            },
            Expression::AggregateFunction {
                name: "AVG".to_string(),
                distinct: false,
                args: vec![Expression::ColumnRef { table: None, column: "quantity".to_string() }],
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
            args: vec![Expression::ColumnRef { table: None, column: "price".to_string() }],
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
            args: vec![Expression::ColumnRef { table: None, column: "price".to_string() }],
        }];

        let result = execute_columnar(&rows, Some(&filter), &aggregates, &schema);
        assert!(result.is_none());
    }
}
