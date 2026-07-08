//! Columnar filtering - efficient predicate evaluation on column data
//!
//! Note: Some loops use explicit index iteration for better cache locality
//! and auto-vectorization opportunities.

#![allow(clippy::needless_range_loop)]

mod comparison;
mod evaluation;
mod predicates;

// Re-export public types and functions
pub(super) use comparison::parse_date_string;
pub use evaluation::{
    evaluate_column_compare, evaluate_computed_compare_value, evaluate_predicate,
    evaluate_predicate_tree,
};
pub use predicates::{
    collect_referenced_columns, extract_column_predicates, extract_derived_expr,
    extract_full_coverage_predicates, extract_predicate_tree, remap_predicates, ColumnPredicate,
    CompareOp, DerivedExpr, PredicateTree,
};

use crate::{
    errors::ExecutorError,
    select::vectorized::{DEFAULT_BATCH_SIZE, SMALL_BATCH_SIZE},
};

/// Apply a filter to row indices based on a predicate tree
///
/// Returns a bitmap of which rows pass the filter.
/// Supports complex nested AND/OR logic.
///
/// # Arguments
///
/// * `row_count` - Total number of rows
/// * `tree` - Predicate tree to evaluate
/// * `get_value` - Closure to get a value at (row_index, column_index)
///
/// # Returns
///
/// A Vec<bool> where true means the row passes the filter
pub fn create_filter_bitmap_tree<'a, F>(
    row_count: usize,
    tree: &PredicateTree,
    mut get_value: F,
) -> Result<Vec<bool>, ExecutorError>
where
    F: FnMut(usize, usize) -> Option<&'a vibesql_types::SqlValue>,
{
    // Pre-allocate bitmap with all false
    let mut bitmap = vec![false; row_count];

    // Process in batches for better cache locality and potential auto-vectorization
    // SMALL_BATCH_SIZE (256) chosen to fit in L1 cache (~32KB for row indices + column data)
    // This helps with issue #2397: SQLLogicTest queries scanning 1000-row tables
    for batch_start in (0..row_count).step_by(SMALL_BATCH_SIZE) {
        let batch_end = (batch_start + SMALL_BATCH_SIZE).min(row_count);

        // Evaluate batch - compiler can potentially auto-vectorize inner loops
        for row_idx in batch_start..batch_end {
            bitmap[row_idx] = evaluate_predicate_tree(tree, |col_idx| get_value(row_idx, col_idx));
        }
    }

    Ok(bitmap)
}

/// Apply a filter to row indices based on column predicates (legacy)
///
/// Returns a bitmap of which rows pass the filter.
/// This avoids creating intermediate Row objects.
/// For OR support, use `create_filter_bitmap_tree`.
///
/// # Arguments
///
/// * `row_count` - Total number of rows
/// * `predicates` - Column-based predicates to evaluate (implicitly ANDed)
/// * `get_value` - Closure to get a value at (row_index, column_index)
///
/// # Returns
///
/// A Vec<bool> where true means the row passes the filter
pub fn create_filter_bitmap<'a, F>(
    row_count: usize,
    predicates: &[ColumnPredicate],
    mut get_value: F,
) -> Result<Vec<bool>, ExecutorError>
where
    F: FnMut(usize, usize) -> Option<&'a vibesql_types::SqlValue>,
{
    // If no predicates, all rows pass
    if predicates.is_empty() {
        return Ok(vec![true; row_count]);
    }

    let mut bitmap = vec![true; row_count];

    // Evaluate each row against all predicates (AND logic)
    for row_idx in 0..row_count {
        for predicate in predicates.iter() {
            // Handle ColumnCompare specially - needs two column values
            if let ColumnPredicate::ColumnCompare { left_column_idx, op, right_column_idx } =
                predicate
            {
                let left_val = get_value(row_idx, *left_column_idx);
                let right_val = get_value(row_idx, *right_column_idx);
                if !evaluate_column_compare(*op, left_val, right_val) {
                    bitmap[row_idx] = false;
                    break;
                }
                continue;
            }

            // Handle computed-column comparison: materialize the derived value
            // from the referenced columns via the row-path arithmetic
            // evaluator, then compare (issue #5994).
            if let ColumnPredicate::ComputedCompare { expr, op, value, .. } = predicate {
                let mut fetch = |idx: usize| get_value(row_idx, idx).cloned();
                let passes = match expr.evaluate_row(&mut fetch) {
                    Ok(derived) => {
                        evaluation::evaluate_computed_compare_value(&derived, *op, value)
                    }
                    Err(_) => false,
                };
                if !passes {
                    bitmap[row_idx] = false;
                    break;
                }
                continue;
            }

            // Handle null tests specially - they read null-ness directly instead
            // of failing on NULL like value comparisons do. `get_value` returns
            // None for an absent value and Some(SqlValue::Null) for a stored NULL.
            if let ColumnPredicate::IsNull { column_idx }
            | ColumnPredicate::IsNotNull { column_idx } = predicate
            {
                let is_null = matches!(
                    get_value(row_idx, *column_idx),
                    None | Some(vibesql_types::SqlValue::Null)
                );
                let want_null = matches!(predicate, ColumnPredicate::IsNull { .. });
                if is_null != want_null {
                    bitmap[row_idx] = false;
                    break;
                }
                continue;
            }

            let column_idx = match predicate {
                ColumnPredicate::LessThan { column_idx, .. } => *column_idx,
                ColumnPredicate::GreaterThan { column_idx, .. } => *column_idx,
                ColumnPredicate::GreaterThanOrEqual { column_idx, .. } => *column_idx,
                ColumnPredicate::LessThanOrEqual { column_idx, .. } => *column_idx,
                ColumnPredicate::Equal { column_idx, .. } => *column_idx,
                ColumnPredicate::NotEqual { column_idx, .. } => *column_idx,
                ColumnPredicate::Between { column_idx, .. } => *column_idx,
                ColumnPredicate::Like { column_idx, .. } => *column_idx,
                ColumnPredicate::InList { column_idx, .. } => *column_idx,
                // Handled above.
                ColumnPredicate::ColumnCompare { .. }
                | ColumnPredicate::IsNull { .. }
                | ColumnPredicate::IsNotNull { .. }
                | ColumnPredicate::ComputedCompare { .. } => unreachable!(),
            };

            if let Some(value) = get_value(row_idx, column_idx) {
                let result = evaluate_predicate(predicate, value);
                if !result {
                    bitmap[row_idx] = false;
                    break; // Short-circuit: row failed, skip remaining predicates
                }
            } else {
                // NULL values fail all predicates
                bitmap[row_idx] = false;
                break;
            }
        }
    }

    Ok(bitmap)
}

/// Apply a columnar filter using a pre-computed bitmap
///
/// This is a convenience function that creates a filter bitmap
/// and returns the indices of rows that pass.
///
/// # Arguments
///
/// * `rows` - The rows to filter
/// * `predicates` - Column-based predicates to evaluate
///
/// # Returns
///
/// Indices of rows that pass all predicates
pub fn apply_columnar_filter(
    rows: &[vibesql_storage::Row],
    predicates: &[ColumnPredicate],
) -> Result<Vec<usize>, ExecutorError> {
    // For larger row sets, use SIMD-accelerated streaming batch filter
    // Threshold chosen based on benchmark: SIMD overhead pays off around 256+ rows
    const SIMD_THRESHOLD: usize = 256;

    if rows.len() >= SIMD_THRESHOLD && !predicates.is_empty() {
        return apply_columnar_filter_simd_streaming(rows, predicates);
    }

    // For small row sets, use scalar evaluation (lower overhead)
    let bitmap = create_filter_bitmap(rows.len(), predicates, |row_idx, col_idx| {
        rows.get(row_idx).and_then(|row| row.get(col_idx))
    })?;
    Ok(bitmap
        .iter()
        .enumerate()
        .filter_map(|(idx, &pass)| if pass { Some(idx) } else { None })
        .collect())
}

/// SIMD-accelerated streaming batch filter
///
/// Processes rows in fixed-size batches, converting each batch to columnar format
/// and applying vectorized SIMD filtering. This approach:
/// - Uses O(batch_size) memory instead of O(total_rows)
/// - Benefits from cache locality (batch fits in L2/L3 cache)
/// - Enables SIMD vectorization for predicate evaluation
///
/// # Arguments
///
/// * `rows` - The rows to filter
/// * `predicates` - Column-based predicates to evaluate
///
/// # Returns
///
/// Indices of rows that pass all predicates
pub fn apply_columnar_filter_simd_streaming(
    rows: &[vibesql_storage::Row],
    predicates: &[ColumnPredicate],
) -> Result<Vec<usize>, ExecutorError> {
    use super::batch::ColumnarBatch;

    if predicates.is_empty() {
        // No predicates: all rows pass
        return Ok((0..rows.len()).collect());
    }

    if rows.is_empty() {
        return Ok(vec![]);
    }

    // OPTIMIZATION: Selective column extraction (#4XXX)
    // Instead of extracting all columns (expensive for wide tables),
    // only extract the columns referenced by predicates.
    //
    // For lineitem (16 columns) with filter on L_RETURNFLAG:
    // - Old: Extract 16 columns × N rows = 16N values
    // - New: Extract 1 column × N rows = 1N values (16x reduction)
    let referenced_columns = collect_referenced_columns(predicates);
    let remapped_predicates = remap_predicates(predicates, &referenced_columns);

    // Batch size tuned for L2 cache efficiency (~256KB)
    // With selective extraction, we use fewer columns so batches are smaller.
    // DEFAULT_BATCH_SIZE (1024) provides good balance of throughput and memory.
    let mut matching_indices = Vec::with_capacity(rows.len() / 4); // Estimate 25% selectivity

    // Process rows in streaming batches
    for batch_start in (0..rows.len()).step_by(DEFAULT_BATCH_SIZE) {
        let batch_end = (batch_start + DEFAULT_BATCH_SIZE).min(rows.len());
        let batch_slice = &rows[batch_start..batch_end];

        // Convert only the referenced columns to columnar format
        // This is the key optimization - extracting 1-2 columns instead of 16
        let batch = ColumnarBatch::from_rows_selective(batch_slice, &referenced_columns)?;

        // Create filter mask using SIMD operations with remapped predicates
        let filter_mask = create_filter_mask_simd(&batch, &remapped_predicates)?;

        // Collect matching indices
        for (local_idx, &passes) in filter_mask.iter().enumerate() {
            if passes {
                matching_indices.push(batch_start + local_idx);
            }
        }
    }

    Ok(matching_indices)
}

/// Create a filter mask using SIMD operations
///
/// Returns a Vec<bool> where true means the row passes all predicates.
fn create_filter_mask_simd(
    batch: &super::batch::ColumnarBatch,
    predicates: &[ColumnPredicate],
) -> Result<Vec<bool>, ExecutorError> {
    use super::simd_filter::simd_create_filter_mask;
    simd_create_filter_mask(batch, predicates)
}

/// Filter rows in place using columnar predicates
///
/// Returns a new Vec containing only the rows that pass all predicates.
/// This is the main entry point for columnar filtering.
///
/// # Arguments
///
/// * `rows` - The rows to filter
/// * `predicates` - Column-based predicates to evaluate
///
/// # Returns
///
/// Filtered rows
pub fn filter_rows(
    rows: Vec<vibesql_storage::Row>,
    predicates: &[ColumnPredicate],
) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
    if predicates.is_empty() {
        return Ok(rows);
    }

    let indices = apply_columnar_filter(&rows, predicates)?;
    Ok(indices.into_iter().filter_map(|idx| rows.get(idx).cloned()).collect())
}

#[cfg(test)]
mod tests {
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    use super::*;

    #[test]
    fn test_simd_streaming_filter_large_dataset() {
        // Create a large dataset (>256 rows to trigger SIMD path)
        let rows: Vec<Row> = (0..1000)
            .map(|i| Row::new(vec![SqlValue::Integer(i), SqlValue::Double(i as f64 * 0.01)]))
            .collect();

        // Filter: col0 < 500 AND col1 < 3.0
        let predicates = vec![
            ColumnPredicate::LessThan { column_idx: 0, value: SqlValue::Integer(500) },
            ColumnPredicate::LessThan { column_idx: 1, value: SqlValue::Double(3.0) },
        ];

        let indices = apply_columnar_filter(&rows, &predicates).unwrap();

        // Verify: indices should be 0..300 (where i < 500 AND i*0.01 < 3.0)
        // i*0.01 < 3.0 => i < 300
        assert_eq!(indices.len(), 300);
        for (i, idx) in indices.iter().enumerate() {
            assert_eq!(*idx, i);
        }
    }

    #[test]
    fn test_simd_streaming_filter_date_predicates() {
        use vibesql_types::Date;

        // Create rows with dates spanning multiple years
        let rows: Vec<Row> = (0..500)
            .map(|i| {
                let year = 1994 + (i / 365);
                let day = (i % 28) + 1;
                Row::new(vec![SqlValue::Date(Date { year, month: 6, day: day as u8 })])
            })
            .collect();

        // Filter: date >= 1994-06-01 AND date < 1995-06-01
        let predicates = vec![
            ColumnPredicate::GreaterThanOrEqual {
                column_idx: 0,
                value: SqlValue::Date(Date { year: 1994, month: 6, day: 1 }),
            },
            ColumnPredicate::LessThan {
                column_idx: 0,
                value: SqlValue::Date(Date { year: 1995, month: 6, day: 1 }),
            },
        ];

        let indices = apply_columnar_filter(&rows, &predicates).unwrap();

        // Should match rows where year is 1994 (indices 0-364)
        assert_eq!(indices.len(), 365);
    }

    #[test]
    fn test_simd_streaming_vs_scalar_consistency() {
        // Verify SIMD streaming produces same results as scalar path
        let rows: Vec<Row> = (0..300).map(|i| Row::new(vec![SqlValue::Integer(i)])).collect();

        let predicates = vec![
            ColumnPredicate::GreaterThan { column_idx: 0, value: SqlValue::Integer(100) },
            ColumnPredicate::LessThan { column_idx: 0, value: SqlValue::Integer(200) },
        ];

        // Get results from SIMD streaming path
        let simd_indices = apply_columnar_filter_simd_streaming(&rows, &predicates).unwrap();

        // Get results from scalar path (using smaller dataset)
        let small_rows: Vec<Row> =
            (0..100).map(|i| Row::new(vec![SqlValue::Integer(i + 100)])).collect();
        let scalar_bitmap =
            create_filter_bitmap(small_rows.len(), &predicates, |row_idx, col_idx| {
                small_rows.get(row_idx).and_then(|row| row.get(col_idx))
            })
            .unwrap();
        let expected_count = scalar_bitmap.iter().filter(|&&x| x).count();

        // Both should find 99 rows (101-199 inclusive for SIMD, 100-199 adjusted for scalar)
        assert_eq!(simd_indices.len(), 99); // 101, 102, ..., 199
        assert_eq!(expected_count, 99); // Same count
    }

    #[test]
    fn test_predicate_tree_or() {
        // Test: (col0 < 10 OR col0 > 20)
        let tree = PredicateTree::Or(vec![
            PredicateTree::Leaf(ColumnPredicate::LessThan {
                column_idx: 0,
                value: SqlValue::Integer(10),
            }),
            PredicateTree::Leaf(ColumnPredicate::GreaterThan {
                column_idx: 0,
                value: SqlValue::Integer(20),
            }),
        ]);

        // Test with value < 10 (should pass via first condition)
        assert!(evaluate_predicate_tree(&tree, |idx| {
            if idx == 0 {
                Some(&SqlValue::Integer(5))
            } else {
                None
            }
        }));

        // Test with value > 20 (should pass via second condition)
        assert!(evaluate_predicate_tree(&tree, |idx| {
            if idx == 0 {
                Some(&SqlValue::Integer(25))
            } else {
                None
            }
        }));

        // Test with value in middle (should fail both conditions)
        assert!(!evaluate_predicate_tree(&tree, |idx| {
            if idx == 0 {
                Some(&SqlValue::Integer(15))
            } else {
                None
            }
        }));
    }

    #[test]
    fn test_predicate_tree_complex() {
        // Test: ((col0 < 10 OR col1 > 20) AND col2 = 5)
        // This mirrors the structure from issue #2397
        let tree = PredicateTree::And(vec![
            PredicateTree::Or(vec![
                PredicateTree::Leaf(ColumnPredicate::LessThan {
                    column_idx: 0,
                    value: SqlValue::Integer(10),
                }),
                PredicateTree::Leaf(ColumnPredicate::GreaterThan {
                    column_idx: 1,
                    value: SqlValue::Integer(20),
                }),
            ]),
            PredicateTree::Leaf(ColumnPredicate::Equal {
                column_idx: 2,
                value: SqlValue::Integer(5),
            }),
        ]);

        let rows = [
            // Row 0: col0=5, col1=15, col2=5 -> (5<10 OR 15>20) AND 5=5 -> TRUE AND TRUE -> TRUE
            Row::new(vec![SqlValue::Integer(5), SqlValue::Integer(15), SqlValue::Integer(5)]),
            // Row 1: col0=15, col1=25, col2=5 -> (15<10 OR 25>20) AND 5=5 -> TRUE AND TRUE -> TRUE
            Row::new(vec![SqlValue::Integer(15), SqlValue::Integer(25), SqlValue::Integer(5)]),
            // Row 2: col0=15, col1=15, col2=5 -> (15<10 OR 15>20) AND 5=5 -> FALSE AND TRUE ->
            // FALSE
            Row::new(vec![SqlValue::Integer(15), SqlValue::Integer(15), SqlValue::Integer(5)]),
            // Row 3: col0=5, col1=25, col2=10 -> (5<10 OR 25>20) AND 10=5 -> TRUE AND FALSE ->
            // FALSE
            Row::new(vec![SqlValue::Integer(5), SqlValue::Integer(25), SqlValue::Integer(10)]),
        ];

        let bitmap = create_filter_bitmap_tree(rows.len(), &tree, |row_idx, col_idx| {
            rows.get(row_idx).and_then(|row| row.get(col_idx))
        })
        .unwrap();

        assert_eq!(bitmap, vec![true, true, false, false]);
    }

    #[test]
    fn test_extract_predicate_tree_or() {
        use vibesql_ast::{BinaryOperator, Expression};
        use vibesql_catalog::{ColumnSchema, TableSchema};
        use vibesql_types::DataType;

        use crate::schema::CombinedSchema;

        let schema = TableSchema::new(
            "test".to_string(),
            vec![
                ColumnSchema::new("col0".to_string(), DataType::Integer, false),
                ColumnSchema::new("col1".to_string(), DataType::Integer, false),
            ],
        );
        let schema = CombinedSchema::from_table("test".to_string(), schema);

        // Build: col0 < 10 OR col1 > 20
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                    "col0", false,
                ))),
                op: BinaryOperator::LessThan,
                right: Box::new(Expression::Literal(SqlValue::Integer(10))),
            }),
            op: BinaryOperator::Or,
            right: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                    "col1", false,
                ))),
                op: BinaryOperator::GreaterThan,
                right: Box::new(Expression::Literal(SqlValue::Integer(20))),
            }),
        };

        let tree = extract_predicate_tree(&expr, &schema, false);
        assert!(tree.is_some());

        let tree = tree.unwrap();
        match tree {
            PredicateTree::Or(children) => {
                assert_eq!(children.len(), 2);
            }
            _ => panic!("Expected Or node"),
        }
    }

    #[test]
    fn test_less_than_predicate() {
        let pred = ColumnPredicate::LessThan { column_idx: 0, value: SqlValue::Integer(10) };

        assert!(evaluate_predicate(&pred, &SqlValue::Integer(5)));
        assert!(!evaluate_predicate(&pred, &SqlValue::Integer(10)));
        assert!(!evaluate_predicate(&pred, &SqlValue::Integer(15)));
    }

    #[test]
    fn test_between_predicate() {
        let pred = ColumnPredicate::Between {
            column_idx: 0,
            low: SqlValue::Double(0.05),
            high: SqlValue::Double(0.07),
        };

        assert!(evaluate_predicate(&pred, &SqlValue::Double(0.06)));
        assert!(evaluate_predicate(&pred, &SqlValue::Double(0.05)));
        assert!(evaluate_predicate(&pred, &SqlValue::Double(0.07)));
        assert!(!evaluate_predicate(&pred, &SqlValue::Double(0.04)));
        assert!(!evaluate_predicate(&pred, &SqlValue::Double(0.08)));
    }

    // ---- Issue #5994: computed-column comparison parity ----

    use predicates::DerivedExpr;

    /// Build a `ComputedCompare` for `col0 * col1 op value`.
    fn mul_compare(op: CompareOp, value: SqlValue) -> ColumnPredicate {
        ColumnPredicate::ComputedCompare {
            expr: DerivedExpr::BinaryOp {
                left: Box::new(DerivedExpr::Column(0)),
                op: vibesql_ast::BinaryOperator::Multiply,
                right: Box::new(DerivedExpr::Column(1)),
            },
            op,
            value,
            columns: vec![0, 1],
        }
    }

    /// Reference: row-by-row `col0 * col1 > 100` using i128 to avoid overflow
    /// ambiguity in the oracle (real i64 overflow is covered separately).
    fn reference_mul_gt(rows: &[Row], threshold: i64) -> Vec<usize> {
        rows.iter()
            .enumerate()
            .filter_map(|(i, r)| {
                let a = match r.get(0) {
                    Some(SqlValue::Integer(v)) => Some(*v as i128),
                    _ => None,
                }?;
                let b = match r.get(1) {
                    Some(SqlValue::Integer(v)) => Some(*v as i128),
                    _ => None,
                }?;
                if a * b > threshold as i128 {
                    Some(i)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Both sides of the SIMD row-count threshold (256) must produce the same
    /// result as a scalar row-by-row reference for `col0 * col1 > 100`.
    #[test]
    fn test_computed_compare_simd_vs_reference_both_thresholds() {
        for n in [50usize, 1000usize] {
            let rows: Vec<Row> = (0..n)
                .map(|i| {
                    Row::new(vec![
                        SqlValue::Integer((i % 20) as i64),
                        SqlValue::Integer((i % 13) as i64),
                    ])
                })
                .collect();
            let predicates = vec![mul_compare(CompareOp::GreaterThan, SqlValue::Integer(100))];
            let got = apply_columnar_filter(&rows, &predicates).unwrap();
            let want = reference_mul_gt(&rows, 100);
            assert_eq!(got, want, "mismatch at n={n} (threshold boundary)");
        }
    }

    /// A NULL in any input column makes the derived value NULL, which is
    /// non-matching — exactly like the row path.
    #[test]
    fn test_computed_compare_null_propagation() {
        // 400 rows to force the SIMD streaming path.
        let rows: Vec<Row> = (0..400)
            .map(|i| {
                let a = if i % 7 == 0 { SqlValue::Null } else { SqlValue::Integer(11) };
                Row::new(vec![a, SqlValue::Integer(11)])
            })
            .collect();
        // 11 * 11 = 121 > 100 for non-null rows; NULL rows must not match.
        let predicates = vec![mul_compare(CompareOp::GreaterThan, SqlValue::Integer(100))];
        let got = apply_columnar_filter(&rows, &predicates).unwrap();
        let want: Vec<usize> = (0..400).filter(|i| i % 7 != 0).collect();
        assert_eq!(got, want);
    }

    /// i64 overflow in the derived value must fall back to a float result and
    /// compare correctly (parity with the row path), never wrap or panic. Runs
    /// on the SIMD path (>256 rows).
    #[test]
    fn test_computed_compare_overflow_parity() {
        let rows: Vec<Row> = (0..300)
            .map(|i| {
                if i == 0 {
                    // i64::MAX * 2 overflows i64; row path yields a large Double
                    // that is > 0, so this row matches `> 0`.
                    Row::new(vec![SqlValue::Integer(i64::MAX), SqlValue::Integer(2)])
                } else {
                    Row::new(vec![SqlValue::Integer(0), SqlValue::Integer(0)])
                }
            })
            .collect();
        let predicates = vec![mul_compare(CompareOp::GreaterThan, SqlValue::Integer(0))];
        let got = apply_columnar_filter(&rows, &predicates).unwrap();
        // Only row 0 (the overflow row) has a positive product; the rest are 0.
        assert_eq!(got, vec![0]);
    }

    /// The scalar (small-batch) path and the SIMD (large-batch) path agree on
    /// the same logical data for a division predicate (integer division by a
    /// column that may be zero => NULL => non-matching, per row-path parity).
    #[test]
    fn test_computed_compare_division_by_zero_non_matching() {
        // col0 / col1 predicate where col1 is 0 for some rows.
        let make_rows = |n: usize| -> Vec<Row> {
            (0..n)
                .map(|i| {
                    let denom = if i % 5 == 0 { 0 } else { 2 };
                    Row::new(vec![SqlValue::Integer(10), SqlValue::Integer(denom)])
                })
                .collect()
        };
        let div_pred = |op: CompareOp, value: SqlValue| ColumnPredicate::ComputedCompare {
            expr: DerivedExpr::BinaryOp {
                left: Box::new(DerivedExpr::Column(0)),
                op: vibesql_ast::BinaryOperator::Divide,
                right: Box::new(DerivedExpr::Column(1)),
            },
            op,
            value,
            columns: vec![0, 1],
        };

        for n in [40usize, 500usize] {
            let rows = make_rows(n);
            // 10 / 2 = 5 >= 1 (matches); 10 / 0 = NULL (non-matching).
            let predicates = vec![div_pred(CompareOp::GreaterThanOrEqual, SqlValue::Integer(1))];
            let got = apply_columnar_filter(&rows, &predicates).unwrap();
            let want: Vec<usize> = (0..n).filter(|i| i % 5 != 0).collect();
            assert_eq!(got, want, "division-by-zero parity failed at n={n}");
        }
    }

    #[test]
    fn test_filter_bitmap() {
        use vibesql_storage::Row;

        let rows = [
            Row::new(vec![SqlValue::Integer(5)]),
            Row::new(vec![SqlValue::Integer(10)]),
            Row::new(vec![SqlValue::Integer(15)]),
            Row::new(vec![SqlValue::Integer(20)]),
            Row::new(vec![SqlValue::Integer(25)]),
        ];

        // Test with no predicates - all rows should pass
        let bitmap = create_filter_bitmap(rows.len(), &[], |row_idx, col_idx| {
            rows.get(row_idx).and_then(|row| row.get(col_idx))
        })
        .unwrap();
        assert_eq!(bitmap.len(), 5);
        assert!(bitmap.iter().all(|&x| x));

        // Test with LessThan predicate
        let predicates =
            vec![ColumnPredicate::LessThan { column_idx: 0, value: SqlValue::Integer(18) }];
        let bitmap = create_filter_bitmap(rows.len(), &predicates, |row_idx, col_idx| {
            rows.get(row_idx).and_then(|row| row.get(col_idx))
        })
        .unwrap();
        assert_eq!(bitmap, vec![true, true, true, false, false]);
    }
}
