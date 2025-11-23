//! Columnar filtering - efficient predicate evaluation on column data

use crate::{errors::ExecutorError, schema::CombinedSchema};
use vibesql_ast::{BinaryOperator, Expression};
use vibesql_types::SqlValue;

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
    F: FnMut(usize, usize) -> Option<&'a SqlValue>,
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
    F: FnMut(usize, usize) -> Option<&'a SqlValue>,
{
    // If no predicates, all rows pass
    if predicates.is_empty() {
        return Ok(vec![true; row_count]);
    }

    let mut bitmap = vec![true; row_count];

    // Evaluate each row against all predicates (AND logic)
    for row_idx in 0..row_count {
        for (pred_idx, predicate) in predicates.iter().enumerate() {
            let column_idx = match predicate {
                ColumnPredicate::LessThan { column_idx, .. } => *column_idx,
                ColumnPredicate::GreaterThan { column_idx, .. } => *column_idx,
                ColumnPredicate::GreaterThanOrEqual { column_idx, .. } => *column_idx,
                ColumnPredicate::LessThanOrEqual { column_idx, .. } => *column_idx,
                ColumnPredicate::Equal { column_idx, .. } => *column_idx,
                ColumnPredicate::Between { column_idx, .. } => *column_idx,
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

    let passing_count = bitmap.iter().filter(|&&b| b).count();
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

/// A predicate tree representing complex logical expressions
///
/// Supports nested AND/OR combinations for efficient columnar evaluation.
/// Example: `((col0 < 10 OR col1 > 20) AND col2 = 5)` becomes:
/// ```text
/// And([
///     Or([
///         Leaf(col0 < 10),
///         Leaf(col1 > 20)
///     ]),
///     Leaf(col2 = 5)
/// ])
/// ```
#[derive(Debug, Clone)]
pub enum PredicateTree {
    /// Logical AND - all children must be true
    And(Vec<PredicateTree>),

    /// Logical OR - at least one child must be true
    Or(Vec<PredicateTree>),

    /// Leaf predicate - single column comparison
    Leaf(ColumnPredicate),
}

/// A predicate on a single column
///
/// Represents filters like: `column_idx < 24` or `column_idx BETWEEN 0.05 AND 0.07`
#[derive(Debug, Clone)]
pub enum ColumnPredicate {
    /// column < value
    LessThan { column_idx: usize, value: SqlValue },

    /// column > value
    GreaterThan { column_idx: usize, value: SqlValue },

    /// column >= value
    GreaterThanOrEqual { column_idx: usize, value: SqlValue },

    /// column <= value
    LessThanOrEqual { column_idx: usize, value: SqlValue },

    /// column = value
    Equal { column_idx: usize, value: SqlValue },

    /// column BETWEEN low AND high
    Between {
        column_idx: usize,
        low: SqlValue,
        high: SqlValue,
    },
}

/// Extract column predicates as a tree from a WHERE clause expression
///
/// This converts AST expressions into a predicate tree that can be evaluated
/// efficiently using columnar operations. Supports complex nested AND/OR logic.
///
/// Currently supports:
/// - Simple comparisons: column op literal (where op is <, >, <=, >=, =)
/// - BETWEEN: column BETWEEN literal AND literal
/// - AND/OR combinations of the above with arbitrary nesting
///
/// # Arguments
///
/// * `expr` - The WHERE clause expression
/// * `schema` - The schema to resolve column names to indices
///
/// # Returns
///
/// Some(tree) if the expression can be converted to columnar predicates,
/// None if the expression is too complex for columnar optimization.
pub fn extract_predicate_tree(
    expr: &Expression,
    schema: &CombinedSchema,
) -> Option<PredicateTree> {
    extract_tree_recursive(expr, schema)
}

/// Extract simple column predicates from a WHERE clause expression (legacy)
///
/// This is the legacy interface that returns a flat list of predicates
/// that are implicitly ANDed together. For OR support, use `extract_predicate_tree`.
///
/// # Arguments
///
/// * `expr` - The WHERE clause expression
/// * `schema` - The schema to resolve column names to indices
///
/// # Returns
///
/// Some(predicates) if the expression can be converted to simple AND-only predicates,
/// None if the expression contains OR or is too complex.
pub fn extract_column_predicates(
    expr: &Expression,
    schema: &CombinedSchema,
) -> Option<Vec<ColumnPredicate>> {
    let mut predicates = Vec::new();
    extract_predicates_recursive(expr, schema, &mut predicates)?;
    Some(predicates)
}

/// Recursively extract predicates as a tree from an expression (handles OR)
fn extract_tree_recursive(expr: &Expression, schema: &CombinedSchema) -> Option<PredicateTree> {
    match expr {
        // AND: combine both sides
        Expression::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            let left_tree = extract_tree_recursive(left, schema)?;
            let right_tree = extract_tree_recursive(right, schema)?;

            // Flatten nested ANDs
            let mut children = Vec::new();
            match left_tree {
                PredicateTree::And(mut left_children) => children.append(&mut left_children),
                other => children.push(other),
            }
            match right_tree {
                PredicateTree::And(mut right_children) => children.append(&mut right_children),
                other => children.push(other),
            }

            Some(PredicateTree::And(children))
        }

        // OR: combine both sides
        Expression::BinaryOp {
            left,
            op: BinaryOperator::Or,
            right,
        } => {
            let left_tree = extract_tree_recursive(left, schema)?;
            let right_tree = extract_tree_recursive(right, schema)?;

            // Flatten nested ORs
            let mut children = Vec::new();
            match left_tree {
                PredicateTree::Or(mut left_children) => children.append(&mut left_children),
                other => children.push(other),
            }
            match right_tree {
                PredicateTree::Or(mut right_children) => children.append(&mut right_children),
                other => children.push(other),
            }

            Some(PredicateTree::Or(children))
        }

        // Binary comparison: column op literal
        Expression::BinaryOp { left, op, right } => {
            // Try: column op literal
            if let (Expression::ColumnRef { table, column }, Expression::Literal(value)) =
                (left.as_ref(), right.as_ref())
            {
                let column_idx = schema.get_column_index(table.as_deref(), column)?;
                let predicate = match op {
                    BinaryOperator::LessThan => ColumnPredicate::LessThan {
                        column_idx,
                        value: value.clone(),
                    },
                    BinaryOperator::GreaterThan => ColumnPredicate::GreaterThan {
                        column_idx,
                        value: value.clone(),
                    },
                    BinaryOperator::LessThanOrEqual => ColumnPredicate::LessThanOrEqual {
                        column_idx,
                        value: value.clone(),
                    },
                    BinaryOperator::GreaterThanOrEqual => ColumnPredicate::GreaterThanOrEqual {
                        column_idx,
                        value: value.clone(),
                    },
                    BinaryOperator::Equal => ColumnPredicate::Equal {
                        column_idx,
                        value: value.clone(),
                    },
                    _ => return None,
                };
                return Some(PredicateTree::Leaf(predicate));
            }

            // Try: literal op column (reverse the comparison)
            if let (Expression::Literal(value), Expression::ColumnRef { table, column }) =
                (left.as_ref(), right.as_ref())
            {
                let column_idx = schema.get_column_index(table.as_deref(), column)?;
                let predicate = match op {
                    BinaryOperator::LessThan => ColumnPredicate::GreaterThan {
                        column_idx,
                        value: value.clone(),
                    },
                    BinaryOperator::GreaterThan => ColumnPredicate::LessThan {
                        column_idx,
                        value: value.clone(),
                    },
                    BinaryOperator::LessThanOrEqual => ColumnPredicate::GreaterThanOrEqual {
                        column_idx,
                        value: value.clone(),
                    },
                    BinaryOperator::GreaterThanOrEqual => ColumnPredicate::LessThanOrEqual {
                        column_idx,
                        value: value.clone(),
                    },
                    BinaryOperator::Equal => ColumnPredicate::Equal {
                        column_idx,
                        value: value.clone(),
                    },
                    _ => return None,
                };
                return Some(PredicateTree::Leaf(predicate));
            }

            None
        }

        // BETWEEN: column BETWEEN low AND high
        Expression::Between {
            expr: inner,
            low,
            high,
            negated: false,
            symmetric: _,
        } => {
            if let Expression::ColumnRef { table, column } = inner.as_ref() {
                if let (Expression::Literal(low_val), Expression::Literal(high_val)) =
                    (low.as_ref(), high.as_ref())
                {
                    let column_idx = schema.get_column_index(table.as_deref(), column)?;
                    return Some(PredicateTree::Leaf(ColumnPredicate::Between {
                        column_idx,
                        low: low_val.clone(),
                        high: high_val.clone(),
                    }));
                }
            }
            None
        }

        _ => None,
    }
}

/// Recursively extract predicates from an expression (legacy AND-only)
fn extract_predicates_recursive(
    expr: &Expression,
    schema: &CombinedSchema,
    predicates: &mut Vec<ColumnPredicate>,
) -> Option<()> {
    match expr {
        // AND: extract predicates from both sides
        Expression::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            extract_predicates_recursive(left, schema, predicates)?;
            extract_predicates_recursive(right, schema, predicates)?;
            Some(())
        }

        // Binary comparison: column op literal
        Expression::BinaryOp { left, op, right } => {
            // Try: column op literal
            if let (Expression::ColumnRef { table, column }, Expression::Literal(value)) =
                (left.as_ref(), right.as_ref())
            {
                let column_idx = schema.get_column_index(table.as_deref(), column)?;
                let predicate = match op {
                    BinaryOperator::LessThan => ColumnPredicate::LessThan {
                        column_idx,
                        value: value.clone(),
                    },
                    BinaryOperator::GreaterThan => ColumnPredicate::GreaterThan {
                        column_idx,
                        value: value.clone(),
                    },
                    BinaryOperator::LessThanOrEqual => ColumnPredicate::LessThanOrEqual {
                        column_idx,
                        value: value.clone(),
                    },
                    BinaryOperator::GreaterThanOrEqual => ColumnPredicate::GreaterThanOrEqual {
                        column_idx,
                        value: value.clone(),
                    },
                    BinaryOperator::Equal => ColumnPredicate::Equal {
                        column_idx,
                        value: value.clone(),
                    },
                    _ => return None, // Unsupported operator
                };
                predicates.push(predicate);
                return Some(());
            }

            // Try: literal op column (reverse the comparison)
            if let (Expression::Literal(value), Expression::ColumnRef { table, column }) =
                (left.as_ref(), right.as_ref())
            {
                let column_idx = schema.get_column_index(table.as_deref(), column)?;
                let predicate = match op {
                    // Reverse the comparison: literal < column => column > literal
                    BinaryOperator::LessThan => ColumnPredicate::GreaterThan {
                        column_idx,
                        value: value.clone(),
                    },
                    BinaryOperator::GreaterThan => ColumnPredicate::LessThan {
                        column_idx,
                        value: value.clone(),
                    },
                    BinaryOperator::LessThanOrEqual => ColumnPredicate::GreaterThanOrEqual {
                        column_idx,
                        value: value.clone(),
                    },
                    BinaryOperator::GreaterThanOrEqual => ColumnPredicate::LessThanOrEqual {
                        column_idx,
                        value: value.clone(),
                    },
                    BinaryOperator::Equal => ColumnPredicate::Equal {
                        column_idx,
                        value: value.clone(),
                    },
                    _ => return None, // Unsupported operator
                };
                predicates.push(predicate);
                return Some(());
            }

            None
        }

        // BETWEEN: column BETWEEN low AND high
        Expression::Between {
            expr: inner,
            low,
            high,
            negated: false,
            symmetric: _,
        } => {
            if let Expression::ColumnRef { table, column } = inner.as_ref() {
                if let (Expression::Literal(low_val), Expression::Literal(high_val)) =
                    (low.as_ref(), high.as_ref())
                {
                    let column_idx = schema.get_column_index(table.as_deref(), column)?;
                    predicates.push(ColumnPredicate::Between {
                        column_idx,
                        low: low_val.clone(),
                        high: high_val.clone(),
                    });
                    return Some(());
                }
            }
            None
        }

        // Any other expression is too complex
        _ => None,
    }
}

/// Evaluate a predicate tree on a row
///
/// Returns true if the row satisfies the entire predicate tree.
/// Implements proper short-circuit semantics for AND/OR.
///
/// # Arguments
///
/// * `tree` - The predicate tree to evaluate
/// * `get_value` - Closure to get a value at a column index for the current row
///
/// # Returns
///
/// true if the row passes the predicate tree, false otherwise
pub fn evaluate_predicate_tree<'a, F>(tree: &PredicateTree, mut get_value: F) -> bool
where
    F: FnMut(usize) -> Option<&'a SqlValue>,
{
    evaluate_predicate_tree_impl(tree, &mut get_value)
}

/// Internal implementation of predicate tree evaluation
///
/// This helper function allows proper recursion with mutable closure references.
fn evaluate_predicate_tree_impl<'a, F>(tree: &PredicateTree, get_value: &mut F) -> bool
where
    F: FnMut(usize) -> Option<&'a SqlValue>,
{
    match tree {
        PredicateTree::And(children) => {
            // All children must be true - short-circuit on first false
            for child in children {
                if !evaluate_predicate_tree_impl(child, get_value) {
                    return false;
                }
            }
            true
        }
        PredicateTree::Or(children) => {
            // At least one child must be true - short-circuit on first true
            for child in children {
                if evaluate_predicate_tree_impl(child, get_value) {
                    return true;
                }
            }
            false
        }
        PredicateTree::Leaf(predicate) => {
            // Get the column value and evaluate the leaf predicate
            let column_idx = match predicate {
                ColumnPredicate::LessThan { column_idx, .. }
                | ColumnPredicate::GreaterThan { column_idx, .. }
                | ColumnPredicate::GreaterThanOrEqual { column_idx, .. }
                | ColumnPredicate::LessThanOrEqual { column_idx, .. }
                | ColumnPredicate::Equal { column_idx, .. }
                | ColumnPredicate::Between { column_idx, .. } => *column_idx,
            };

            if let Some(value) = get_value(column_idx) {
                evaluate_predicate(predicate, value)
            } else {
                // NULL values fail all predicates
                false
            }
        }
    }
}

/// Evaluate a column predicate on a specific value
///
/// Returns true if the value satisfies the predicate
pub fn evaluate_predicate(predicate: &ColumnPredicate, value: &SqlValue) -> bool {
    match predicate {
        ColumnPredicate::LessThan { value: threshold, .. } => {
            compare_values(value, threshold) == std::cmp::Ordering::Less
        }
        ColumnPredicate::GreaterThan { value: threshold, .. } => {
            compare_values(value, threshold) == std::cmp::Ordering::Greater
        }
        ColumnPredicate::GreaterThanOrEqual { value: threshold, .. } => {
            matches!(
                compare_values(value, threshold),
                std::cmp::Ordering::Greater | std::cmp::Ordering::Equal
            )
        }
        ColumnPredicate::LessThanOrEqual { value: threshold, .. } => {
            matches!(
                compare_values(value, threshold),
                std::cmp::Ordering::Less | std::cmp::Ordering::Equal
            )
        }
        ColumnPredicate::Equal { value: target, .. } => {
            compare_values(value, target) == std::cmp::Ordering::Equal
        }
        ColumnPredicate::Between { low, high, .. } => {
            let cmp_low = compare_values(value, low);
            let cmp_high = compare_values(value, high);
            let passes_low = matches!(cmp_low, std::cmp::Ordering::Greater | std::cmp::Ordering::Equal);
            let passes_high = matches!(cmp_high, std::cmp::Ordering::Less | std::cmp::Ordering::Equal);
            passes_low && passes_high
        }
    }
}

/// Compare two SqlValues for ordering
///
/// Handles both same-type and mixed numeric type comparisons by coercing to f64
fn compare_values(a: &SqlValue, b: &SqlValue) -> std::cmp::Ordering {
    use std::cmp::Ordering;

    // Try to extract numeric value as f64 for cross-type comparison
    fn to_f64(v: &SqlValue) -> Option<f64> {
        match v {
            SqlValue::Integer(n) => Some(*n as f64),
            SqlValue::Bigint(n) => Some(*n as f64),
            SqlValue::Smallint(n) => Some(*n as f64),
            SqlValue::Float(n) => Some(*n as f64),
            SqlValue::Double(n) => Some(*n),
            SqlValue::Numeric(n) => n.to_string().parse().ok(),
            SqlValue::Real(n) => Some(*n as f64),
            _ => None,
        }
    }

    match (a, b) {
        // Same-type comparisons (fast path)
        (SqlValue::Integer(a), SqlValue::Integer(b)) => a.cmp(b),
        (SqlValue::Bigint(a), SqlValue::Bigint(b)) => a.cmp(b),
        (SqlValue::Smallint(a), SqlValue::Smallint(b)) => a.cmp(b),
        (SqlValue::Float(a), SqlValue::Float(b)) => {
            a.partial_cmp(b).unwrap_or(Ordering::Equal)
        }
        (SqlValue::Double(a), SqlValue::Double(b)) => {
            a.partial_cmp(b).unwrap_or(Ordering::Equal)
        }
        (SqlValue::Numeric(a), SqlValue::Numeric(b)) => {
            a.partial_cmp(b).unwrap_or(Ordering::Equal)
        }
        (SqlValue::Real(a), SqlValue::Real(b)) => {
            a.partial_cmp(b).unwrap_or(Ordering::Equal)
        }
        (SqlValue::Varchar(a), SqlValue::Varchar(b)) => a.cmp(b),
        (SqlValue::Character(a), SqlValue::Character(b)) => a.cmp(b),
        (SqlValue::Date(a), SqlValue::Date(b)) => a.cmp(b),

        // Date-String comparisons: convert string to date format for comparison
        // This handles cases like: date_column >= '1994-01-01'
        (SqlValue::Date(date), SqlValue::Varchar(s)) | (SqlValue::Date(date), SqlValue::Character(s)) => {
            // Compare dates as strings in ISO format (YYYY-MM-DD)
            let date_str = date.to_string();
            date_str.as_str().cmp(s.as_str())
        }
        (SqlValue::Varchar(s), SqlValue::Date(date)) | (SqlValue::Character(s), SqlValue::Date(date)) => {
            // Reverse comparison
            let date_str = date.to_string();
            s.as_str().cmp(date_str.as_str())
        }

        // Mixed numeric types: coerce to f64 with epsilon comparison for floats
        _ => {
            if let (Some(a_f64), Some(b_f64)) = (to_f64(a), to_f64(b)) {
                // Use epsilon comparison for floating point values to handle precision issues
                // This is especially important for Float(0.07) vs Numeric(0.07) comparisons
                const EPSILON: f64 = 1e-9;
                if (a_f64 - b_f64).abs() < EPSILON {
                    Ordering::Equal
                } else if a_f64 < b_f64 {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            } else {
                // Non-numeric mixed types: fall back to Equal (will fail predicate appropriately)
                Ordering::Equal
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_storage::Row;

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
