//! Columnar filtering - efficient predicate evaluation on column data
//!
//! Note: Some loops use explicit index iteration for better cache locality
//! and auto-vectorization opportunities.

#![allow(clippy::needless_range_loop)]

mod comparison;
mod evaluation;
mod predicates;

use crate::errors::ExecutorError;

// Re-export public types and functions
pub(super) use comparison::parse_date_string;
pub use evaluation::{evaluate_predicate, evaluate_predicate_tree};
pub use predicates::{extract_column_predicates, extract_predicate_tree, ColumnPredicate, PredicateTree};

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
    // Batch size of 256 chosen to fit in L1 cache (~32KB for row indices + column data)
    // This helps with issue #2397: SQLLogicTest queries scanning 1000-row tables
    const BATCH_SIZE: usize = 256;

    for batch_start in (0..row_count).step_by(BATCH_SIZE) {
        let batch_end = (batch_start + BATCH_SIZE).min(row_count);

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
            let column_idx = match predicate {
                ColumnPredicate::LessThan { column_idx, .. } => *column_idx,
                ColumnPredicate::GreaterThan { column_idx, .. } => *column_idx,
                ColumnPredicate::GreaterThanOrEqual { column_idx, .. } => *column_idx,
                ColumnPredicate::LessThanOrEqual { column_idx, .. } => *column_idx,
                ColumnPredicate::Equal { column_idx, .. } => *column_idx,
                ColumnPredicate::NotEqual { column_idx, .. } => *column_idx,
                ColumnPredicate::Between { column_idx, .. } => *column_idx,
                ColumnPredicate::Like { column_idx, .. } => *column_idx,
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
    let bitmap = create_filter_bitmap(rows.len(), predicates, |row_idx, col_idx| {
        rows.get(row_idx).and_then(|row| row.get(col_idx))
    })?;
    Ok(bitmap
        .iter()
        .enumerate()
        .filter_map(|(idx, &pass)| if pass { Some(idx) } else { None })
        .collect())
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
    use super::*;
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

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

        let rows = vec![
            // Row 0: col0=5, col1=15, col2=5 -> (5<10 OR 15>20) AND 5=5 -> TRUE AND TRUE -> TRUE
            Row::new(vec![
                SqlValue::Integer(5),
                SqlValue::Integer(15),
                SqlValue::Integer(5),
            ]),
            // Row 1: col0=15, col1=25, col2=5 -> (15<10 OR 25>20) AND 5=5 -> TRUE AND TRUE -> TRUE
            Row::new(vec![
                SqlValue::Integer(15),
                SqlValue::Integer(25),
                SqlValue::Integer(5),
            ]),
            // Row 2: col0=15, col1=15, col2=5 -> (15<10 OR 15>20) AND 5=5 -> FALSE AND TRUE -> FALSE
            Row::new(vec![
                SqlValue::Integer(15),
                SqlValue::Integer(15),
                SqlValue::Integer(5),
            ]),
            // Row 3: col0=5, col1=25, col2=10 -> (5<10 OR 25>20) AND 10=5 -> TRUE AND FALSE -> FALSE
            Row::new(vec![
                SqlValue::Integer(5),
                SqlValue::Integer(25),
                SqlValue::Integer(10),
            ]),
        ];

        let bitmap = create_filter_bitmap_tree(rows.len(), &tree, |row_idx, col_idx| {
            rows.get(row_idx).and_then(|row| row.get(col_idx))
        })
        .unwrap();

        assert_eq!(bitmap, vec![true, true, false, false]);
    }

    #[test]
    fn test_extract_predicate_tree_or() {
        use crate::schema::CombinedSchema;
        use vibesql_ast::{BinaryOperator, Expression};
        use vibesql_catalog::{ColumnSchema, TableSchema};
        use vibesql_types::DataType;

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
                left: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "col0".to_string(),
                }),
                op: BinaryOperator::LessThan,
                right: Box::new(Expression::Literal(SqlValue::Integer(10))),
            }),
            op: BinaryOperator::Or,
            right: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "col1".to_string(),
                }),
                op: BinaryOperator::GreaterThan,
                right: Box::new(Expression::Literal(SqlValue::Integer(20))),
            }),
        };

        let tree = extract_predicate_tree(&expr, &schema);
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
        let pred = ColumnPredicate::LessThan {
            column_idx: 0,
            value: SqlValue::Integer(10),
        };

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

    #[test]
    fn test_filter_bitmap() {
        use vibesql_storage::Row;

        let rows = vec![
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
        let predicates = vec![ColumnPredicate::LessThan {
            column_idx: 0,
            value: SqlValue::Integer(18),
        }];
        let bitmap = create_filter_bitmap(rows.len(), &predicates, |row_idx, col_idx| {
            rows.get(row_idx).and_then(|row| row.get(col_idx))
        })
        .unwrap();
        assert_eq!(bitmap, vec![true, true, true, false, false]);
    }
}
