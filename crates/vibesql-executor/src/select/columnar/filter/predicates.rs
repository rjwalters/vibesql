use vibesql_ast::{BinaryOperator, Expression, UnaryOperator};
use vibesql_types::{SqlMode, SqlValue, TypeAffinity};

use crate::{evaluator::ExpressionEvaluator, schema::CombinedSchema};

/// Comparison operator for column-to-column predicates
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareOp {
    LessThan,
    GreaterThan,
    LessThanOrEqual,
    GreaterThanOrEqual,
    Equal,
    NotEqual,
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

    /// column <> value (not equal)
    NotEqual { column_idx: usize, value: SqlValue },

    /// column BETWEEN low AND high
    Between { column_idx: usize, low: SqlValue, high: SqlValue },

    /// column LIKE pattern
    Like {
        column_idx: usize,
        pattern: String,
        negated: bool,
        case_sensitive: bool,
        escape: Option<char>,
    },

    /// column IN (value1, value2, ...)
    /// The `use_strict_type_ordering` flag indicates whether to use SQLite's strict
    /// type ordering (no coercion) for comparisons. This is true for columns with
    /// NONE or INTEGER affinity, where string values should NOT be coerced to numbers.
    InList {
        column_idx: usize,
        values: Vec<SqlValue>,
        negated: bool,
        use_strict_type_ordering: bool,
    },

    /// column1 op column2 (column-to-column comparison)
    /// Used for predicates like `l_commitdate < l_receiptdate` in TPC-H Q4
    ColumnCompare { left_column_idx: usize, op: CompareOp, right_column_idx: usize },
}

impl ColumnPredicate {
    /// Get all column indices referenced by this predicate
    ///
    /// Returns a vec (typically 1-2 elements) containing the column indices
    /// that need to be extracted for evaluating this predicate.
    pub fn referenced_columns(&self) -> Vec<usize> {
        match self {
            ColumnPredicate::LessThan { column_idx, .. }
            | ColumnPredicate::GreaterThan { column_idx, .. }
            | ColumnPredicate::GreaterThanOrEqual { column_idx, .. }
            | ColumnPredicate::LessThanOrEqual { column_idx, .. }
            | ColumnPredicate::Equal { column_idx, .. }
            | ColumnPredicate::NotEqual { column_idx, .. }
            | ColumnPredicate::Between { column_idx, .. }
            | ColumnPredicate::Like { column_idx, .. }
            | ColumnPredicate::InList { column_idx, .. } => vec![*column_idx],
            ColumnPredicate::ColumnCompare { left_column_idx, right_column_idx, .. } => {
                vec![*left_column_idx, *right_column_idx]
            }
        }
    }
}

/// Collect all unique column indices referenced by a slice of predicates
///
/// Returns a sorted, deduplicated list of column indices that need to be
/// extracted for evaluating the predicates. This enables selective column
/// extraction optimization - only extracting needed columns instead of all.
pub fn collect_referenced_columns(predicates: &[ColumnPredicate]) -> Vec<usize> {
    let mut columns: Vec<usize> = predicates.iter().flat_map(|p| p.referenced_columns()).collect();
    columns.sort_unstable();
    columns.dedup();
    columns
}

/// Remap predicates to use new column indices
///
/// Given predicates that reference columns in the original table and a mapping
/// from original column indices to new (sparse batch) positions, returns new
/// predicates with remapped column indices.
///
/// # Arguments
///
/// * `predicates` - Original predicates with original column indices
/// * `column_mapping` - Sorted array where `column_mapping[new_idx] = old_idx`
///
/// # Returns
///
/// New predicates where each `column_idx` has been replaced with its position
/// in `column_mapping`. Panics if any column_idx is not found in the mapping.
///
/// # Example
///
/// ```text
/// Original predicates: [Equal { column_idx: 14, value: 'R' }]
/// Column mapping: [14]  (only column 14 was extracted)
/// Remapped predicates: [Equal { column_idx: 0, value: 'R' }]
/// ```
pub fn remap_predicates(
    predicates: &[ColumnPredicate],
    column_mapping: &[usize],
) -> Vec<ColumnPredicate> {
    predicates.iter().map(|p| remap_predicate(p, column_mapping)).collect()
}

fn remap_predicate(predicate: &ColumnPredicate, column_mapping: &[usize]) -> ColumnPredicate {
    // Find new index for a column - binary search since mapping is sorted
    let find_new_idx = |old_idx: usize| -> usize {
        column_mapping
            .binary_search(&old_idx)
            .expect("Column index not found in mapping - this is a bug")
    };

    match predicate {
        ColumnPredicate::LessThan { column_idx, value } => ColumnPredicate::LessThan {
            column_idx: find_new_idx(*column_idx),
            value: value.clone(),
        },
        ColumnPredicate::GreaterThan { column_idx, value } => ColumnPredicate::GreaterThan {
            column_idx: find_new_idx(*column_idx),
            value: value.clone(),
        },
        ColumnPredicate::GreaterThanOrEqual { column_idx, value } => {
            ColumnPredicate::GreaterThanOrEqual {
                column_idx: find_new_idx(*column_idx),
                value: value.clone(),
            }
        }
        ColumnPredicate::LessThanOrEqual { column_idx, value } => {
            ColumnPredicate::LessThanOrEqual {
                column_idx: find_new_idx(*column_idx),
                value: value.clone(),
            }
        }
        ColumnPredicate::Equal { column_idx, value } => {
            ColumnPredicate::Equal { column_idx: find_new_idx(*column_idx), value: value.clone() }
        }
        ColumnPredicate::NotEqual { column_idx, value } => ColumnPredicate::NotEqual {
            column_idx: find_new_idx(*column_idx),
            value: value.clone(),
        },
        ColumnPredicate::Between { column_idx, low, high } => ColumnPredicate::Between {
            column_idx: find_new_idx(*column_idx),
            low: low.clone(),
            high: high.clone(),
        },
        ColumnPredicate::Like { column_idx, pattern, negated, case_sensitive, escape } => {
            ColumnPredicate::Like {
                column_idx: find_new_idx(*column_idx),
                pattern: pattern.clone(),
                negated: *negated,
                case_sensitive: *case_sensitive,
                escape: *escape,
            }
        }
        ColumnPredicate::InList { column_idx, values, negated, use_strict_type_ordering } => {
            ColumnPredicate::InList {
                column_idx: find_new_idx(*column_idx),
                values: values.clone(),
                negated: *negated,
                use_strict_type_ordering: *use_strict_type_ordering,
            }
        }
        ColumnPredicate::ColumnCompare { left_column_idx, op, right_column_idx } => {
            ColumnPredicate::ColumnCompare {
                left_column_idx: find_new_idx(*left_column_idx),
                op: *op,
                right_column_idx: find_new_idx(*right_column_idx),
            }
        }
    }
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
pub fn extract_predicate_tree(expr: &Expression, schema: &CombinedSchema) -> Option<PredicateTree> {
    extract_predicate_tree_with_options(expr, schema, false)
}

/// Extract predicate tree with explicit case_sensitive_like setting
pub fn extract_predicate_tree_with_options(
    expr: &Expression,
    schema: &CombinedSchema,
    case_sensitive_like: bool,
) -> Option<PredicateTree> {
    extract_tree_recursive(expr, schema, case_sensitive_like)
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
/// Some(predicates) if the expression can be converted to simple AND-only predicates
/// that reference columns in the schema. Returns None if:
/// - The expression contains OR
/// - No predicates reference columns in the current schema (e.g., all cross-table predicates)
///
/// This function now handles multi-table WHERE clauses by skipping predicates that reference
/// columns not in the schema, allowing columnar optimization for Q3-style queries.
pub fn extract_column_predicates(
    expr: &Expression,
    schema: &CombinedSchema,
    case_sensitive_like: bool,
) -> Option<Vec<ColumnPredicate>> {
    let mut predicates = Vec::new();
    extract_predicates_recursive(expr, schema, &mut predicates, case_sensitive_like)?;
    // Return None if no predicates were extracted (all were cross-table or unsupported)
    // This allows fallback to generic predicate evaluation
    if predicates.is_empty() {
        None
    } else {
        Some(predicates)
    }
}

/// Try to fold a constant expression to a literal value
///
/// Handles simple arithmetic expressions like `1+2`, `10-5`, `2*3`, etc.
/// Returns Some(SqlValue) if the expression can be folded to a constant,
/// or None if it contains column references or other non-constant expressions.
fn try_fold_constant(expr: &Expression) -> Option<SqlValue> {
    match expr {
        // Literals are already folded
        Expression::Literal(val) => Some(val.clone()),

        // Binary operations on constants
        Expression::BinaryOp { left, op, right } => {
            let left_val = try_fold_constant(left)?;
            let right_val = try_fold_constant(right)?;

            // Use the static evaluator with default SQL mode
            ExpressionEvaluator::eval_binary_op_static(
                &left_val,
                op,
                &right_val,
                SqlMode::default(),
            )
            .ok()
        }

        // Unary operations on constants
        Expression::UnaryOp { op, expr: inner } => {
            let inner_val = try_fold_constant(inner)?;

            match op {
                UnaryOperator::Minus => {
                    // Negate the value
                    match inner_val {
                        SqlValue::Integer(n) => Some(SqlValue::Integer(-n)),
                        SqlValue::Bigint(n) => Some(SqlValue::Bigint(-n)),
                        SqlValue::Smallint(n) => Some(SqlValue::Smallint(-n)),
                        SqlValue::Float(n) => Some(SqlValue::Float(-n)),
                        SqlValue::Double(n) => Some(SqlValue::Double(-n)),
                        SqlValue::Real(n) => Some(SqlValue::Real(-n)),
                        SqlValue::Numeric(n) => Some(SqlValue::Numeric(-n)),
                        _ => None,
                    }
                }
                UnaryOperator::Plus => Some(inner_val),
                UnaryOperator::Not => match inner_val {
                    SqlValue::Boolean(b) => Some(SqlValue::Boolean(!b)),
                    _ => None,
                },
                _ => None,
            }
        }

        // Cast expressions can be folded if the inner expression is constant
        Expression::Cast { expr: inner, data_type } => {
            let inner_val = try_fold_constant(inner)?;
            crate::evaluator::casting::cast_value(&inner_val, data_type, &SqlMode::default()).ok()
        }

        // Parenthesized expressions are represented as the inner expression
        // (AST doesn't have a Paren variant, so nothing to do here)

        // Everything else (column refs, functions, etc.) cannot be folded
        _ => None,
    }
}

/// Recursively extract predicates as a tree from an expression (handles OR)
fn extract_tree_recursive(
    expr: &Expression,
    schema: &CombinedSchema,
    case_sensitive_like: bool,
) -> Option<PredicateTree> {
    match expr {
        // AND: combine both sides
        Expression::BinaryOp { left, op: BinaryOperator::And, right } => {
            let left_tree = extract_tree_recursive(left, schema, case_sensitive_like)?;
            let right_tree = extract_tree_recursive(right, schema, case_sensitive_like)?;

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
        Expression::BinaryOp { left, op: BinaryOperator::Or, right } => {
            let left_tree = extract_tree_recursive(left, schema, case_sensitive_like)?;
            let right_tree = extract_tree_recursive(right, schema, case_sensitive_like)?;

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

        // Binary comparison: column op value (value can be literal or foldable expression)
        Expression::BinaryOp { left, op, right } => {
            // Try: column op value (fold right side if possible)
            if let Expression::ColumnRef(col_id) = left.as_ref() {
                let table = col_id.table_canonical();
                let column = col_id.column_canonical();
                if let Some(value) = try_fold_constant(right) {
                    let column_idx = schema.get_column_index(table, column)?;
                    let predicate = match op {
                        BinaryOperator::LessThan => ColumnPredicate::LessThan { column_idx, value },
                        BinaryOperator::GreaterThan => {
                            ColumnPredicate::GreaterThan { column_idx, value }
                        }
                        BinaryOperator::LessThanOrEqual => {
                            ColumnPredicate::LessThanOrEqual { column_idx, value }
                        }
                        BinaryOperator::GreaterThanOrEqual => {
                            ColumnPredicate::GreaterThanOrEqual { column_idx, value }
                        }
                        BinaryOperator::Equal => ColumnPredicate::Equal { column_idx, value },
                        BinaryOperator::NotEqual => ColumnPredicate::NotEqual { column_idx, value },
                        _ => return None,
                    };
                    return Some(PredicateTree::Leaf(predicate));
                }
            }

            // Try: value op column (reverse the comparison, fold left side if possible)
            if let Expression::ColumnRef(col_id) = right.as_ref() {
                let table = col_id.table_canonical();
                let column = col_id.column_canonical();
                if let Some(value) = try_fold_constant(left) {
                    let column_idx = schema.get_column_index(table, column)?;
                    let predicate = match op {
                        BinaryOperator::LessThan => {
                            ColumnPredicate::GreaterThan { column_idx, value }
                        }
                        BinaryOperator::GreaterThan => {
                            ColumnPredicate::LessThan { column_idx, value }
                        }
                        BinaryOperator::LessThanOrEqual => {
                            ColumnPredicate::GreaterThanOrEqual { column_idx, value }
                        }
                        BinaryOperator::GreaterThanOrEqual => {
                            ColumnPredicate::LessThanOrEqual { column_idx, value }
                        }
                        BinaryOperator::Equal => ColumnPredicate::Equal { column_idx, value },
                        // NotEqual is symmetric: literal <> column == column <> literal
                        BinaryOperator::NotEqual => ColumnPredicate::NotEqual { column_idx, value },
                        _ => return None,
                    };
                    return Some(PredicateTree::Leaf(predicate));
                }
            }

            // Try: column op column (column-to-column comparison)
            // This handles predicates like `l_commitdate < l_receiptdate` in TPC-H Q4
            if let (Expression::ColumnRef(col_id1), Expression::ColumnRef(col_id2)) =
                (left.as_ref(), right.as_ref())
            {
                if col_id1.schema_canonical().is_none() && col_id2.schema_canonical().is_none() {
                    let t1 = col_id1.table_canonical();
                    let c1 = col_id1.column_canonical();
                    let t2 = col_id2.table_canonical();
                    let c2 = col_id2.column_canonical();
                    let left_idx = schema.get_column_index(t1, c1)?;
                    let right_idx = schema.get_column_index(t2, c2)?;
                    let compare_op = match op {
                        BinaryOperator::LessThan => CompareOp::LessThan,
                        BinaryOperator::GreaterThan => CompareOp::GreaterThan,
                        BinaryOperator::LessThanOrEqual => CompareOp::LessThanOrEqual,
                        BinaryOperator::GreaterThanOrEqual => CompareOp::GreaterThanOrEqual,
                        BinaryOperator::Equal => CompareOp::Equal,
                        BinaryOperator::NotEqual => CompareOp::NotEqual,
                        _ => return None,
                    };
                    return Some(PredicateTree::Leaf(ColumnPredicate::ColumnCompare {
                        left_column_idx: left_idx,
                        op: compare_op,
                        right_column_idx: right_idx,
                    }));
                }
            }

            None
        }

        // BETWEEN: column BETWEEN low AND high
        // Only support ASYMMETRIC (default) BETWEEN for columnar optimization
        // SYMMETRIC BETWEEN falls through to general evaluator which handles bounds swapping
        Expression::Between { expr: inner, low, high, negated: false, symmetric: false } => {
            if let Expression::ColumnRef(col_id) = inner.as_ref() {
                let table = col_id.table_canonical();
                let column = col_id.column_canonical();
                // Try to fold bounds to literals (handles arithmetic expressions like 1+2)
                let low_val = try_fold_constant(low)?;
                let high_val = try_fold_constant(high)?;

                let column_idx = schema.get_column_index(table, column)?;
                return Some(PredicateTree::Leaf(ColumnPredicate::Between {
                    column_idx,
                    low: low_val,
                    high: high_val,
                }));
            }
            None
        }

        // LIKE: column LIKE pattern
        Expression::Like { expr: inner, pattern, negated, escape } => {
            if let Expression::ColumnRef(col_id) = inner.as_ref() {
                let table = col_id.table_canonical();
                let column = col_id.column_canonical();
                // Extract pattern string from literal
                if let Expression::Literal(SqlValue::Character(pattern_str))
                | Expression::Literal(SqlValue::Varchar(pattern_str)) = pattern.as_ref()
                {
                    // Extract escape character from ESCAPE clause if present
                    let escape_char = escape.as_ref().and_then(|esc_expr| {
                        if let Expression::Literal(SqlValue::Character(s))
                        | Expression::Literal(SqlValue::Varchar(s)) = esc_expr.as_ref()
                        {
                            s.chars().next()
                        } else {
                            None
                        }
                    });

                    let column_idx = schema.get_column_index(table, column)?;
                    return Some(PredicateTree::Leaf(ColumnPredicate::Like {
                        column_idx,
                        pattern: pattern_str.to_string(),
                        negated: *negated,
                        case_sensitive: case_sensitive_like,
                        escape: escape_char,
                    }));
                }
            }
            None
        }

        // IN list: column IN (value1, value2, ...)
        Expression::InList { expr: inner, values, negated } => {
            if let Expression::ColumnRef(col_id) = inner.as_ref() {
                let table = col_id.table_canonical();
                let column = col_id.column_canonical();
                // Extract all values from the IN list (try to fold each to a constant)
                let mut folded_values = Vec::with_capacity(values.len());
                for value_expr in values {
                    if let Some(val) = try_fold_constant(value_expr) {
                        folded_values.push(val);
                    } else {
                        // Non-foldable value in IN list - can't optimize
                        return None;
                    }
                }

                if folded_values.is_empty() {
                    return None;
                }

                let column_idx = schema.get_column_index(table, column)?;

                // Determine if we should use strict type ordering (no coercion).
                // SQLite IN expressions don't coerce strings for NONE or INTEGER affinity.
                // Only REAL affinity coerces strings to numbers in IN expressions.
                let use_strict_type_ordering = schema
                    .get_column_affinity(table, column)
                    .map(|affinity| matches!(affinity, TypeAffinity::None | TypeAffinity::Integer))
                    .unwrap_or(true); // Default to strict ordering if affinity unknown

                return Some(PredicateTree::Leaf(ColumnPredicate::InList {
                    column_idx,
                    values: folded_values,
                    negated: *negated,
                    use_strict_type_ordering,
                }));
            }
            None
        }

        _ => None,
    }
}

/// Recursively extract predicates from an expression (legacy AND-only)
///
/// This function handles multi-table WHERE clauses during single-table scans by
/// skipping predicates that reference columns not in the schema. This allows
/// columnar optimization to work for Q3-style queries with cross-table predicates.
fn extract_predicates_recursive(
    expr: &Expression,
    schema: &CombinedSchema,
    predicates: &mut Vec<ColumnPredicate>,
    case_sensitive_like: bool,
) -> Option<()> {
    match expr {
        // AND: extract predicates from both sides
        // Important: Don't fail if one side can't be extracted - just skip that predicate
        // This allows Q3-style queries where WHERE has both table-local and cross-table predicates
        Expression::BinaryOp { left, op: BinaryOperator::And, right } => {
            // Try both sides - don't propagate failure from either side
            let _ = extract_predicates_recursive(left, schema, predicates, case_sensitive_like);
            let _ = extract_predicates_recursive(right, schema, predicates, case_sensitive_like);
            Some(())
        }

        // Binary comparison: column op value (value can be literal or foldable expression)
        Expression::BinaryOp { left, op, right } => {
            // Try: column op value (fold right side if possible)
            if let Expression::ColumnRef(col_id) = left.as_ref() {
                let table = col_id.table_canonical();
                let column = col_id.column_canonical();
                if let Some(value) = try_fold_constant(right) {
                    // Skip if column not in schema (cross-table predicate)
                    if let Some(column_idx) = schema.get_column_index(table, column) {
                        let predicate = match op {
                            BinaryOperator::LessThan => {
                                ColumnPredicate::LessThan { column_idx, value }
                            }
                            BinaryOperator::GreaterThan => {
                                ColumnPredicate::GreaterThan { column_idx, value }
                            }
                            BinaryOperator::LessThanOrEqual => {
                                ColumnPredicate::LessThanOrEqual { column_idx, value }
                            }
                            BinaryOperator::GreaterThanOrEqual => {
                                ColumnPredicate::GreaterThanOrEqual { column_idx, value }
                            }
                            BinaryOperator::Equal => ColumnPredicate::Equal { column_idx, value },
                            BinaryOperator::NotEqual => {
                                ColumnPredicate::NotEqual { column_idx, value }
                            }
                            _ => return Some(()), // Skip unsupported operator
                        };
                        predicates.push(predicate);
                    }
                    return Some(());
                }
            }

            // Try: value op column (reverse the comparison, fold left side if possible)
            if let Expression::ColumnRef(col_id) = right.as_ref() {
                let table = col_id.table_canonical();
                let column = col_id.column_canonical();
                if let Some(value) = try_fold_constant(left) {
                    // Skip if column not in schema (cross-table predicate)
                    if let Some(column_idx) = schema.get_column_index(table, column) {
                        let predicate = match op {
                            // Reverse the comparison: value < column => column > value
                            BinaryOperator::LessThan => {
                                ColumnPredicate::GreaterThan { column_idx, value }
                            }
                            BinaryOperator::GreaterThan => {
                                ColumnPredicate::LessThan { column_idx, value }
                            }
                            BinaryOperator::LessThanOrEqual => {
                                ColumnPredicate::GreaterThanOrEqual { column_idx, value }
                            }
                            BinaryOperator::GreaterThanOrEqual => {
                                ColumnPredicate::LessThanOrEqual { column_idx, value }
                            }
                            BinaryOperator::Equal => ColumnPredicate::Equal { column_idx, value },
                            // NotEqual is symmetric: value <> column == column <> value
                            BinaryOperator::NotEqual => {
                                ColumnPredicate::NotEqual { column_idx, value }
                            }
                            _ => return Some(()), // Skip unsupported operator
                        };
                        predicates.push(predicate);
                    }
                    return Some(());
                }
            }

            // Try: column op column (column-to-column comparison within same table)
            // This handles predicates like `l_commitdate < l_receiptdate` in TPC-H Q4
            if let (Expression::ColumnRef(col_id1), Expression::ColumnRef(col_id2)) =
                (left.as_ref(), right.as_ref())
            {
                if col_id1.schema_canonical().is_none() && col_id2.schema_canonical().is_none() {
                    let t1 = col_id1.table_canonical();
                    let c1 = col_id1.column_canonical();
                    let t2 = col_id2.table_canonical();
                    let c2 = col_id2.column_canonical();
                    // Only add if BOTH columns are in schema (same-table comparison)
                    if let (Some(left_idx), Some(right_idx)) =
                        (schema.get_column_index(t1, c1), schema.get_column_index(t2, c2))
                    {
                        let compare_op = match op {
                            BinaryOperator::LessThan => CompareOp::LessThan,
                            BinaryOperator::GreaterThan => CompareOp::GreaterThan,
                            BinaryOperator::LessThanOrEqual => CompareOp::LessThanOrEqual,
                            BinaryOperator::GreaterThanOrEqual => CompareOp::GreaterThanOrEqual,
                            BinaryOperator::Equal => CompareOp::Equal,
                            BinaryOperator::NotEqual => CompareOp::NotEqual,
                            _ => return Some(()), // Skip unsupported operator
                        };
                        predicates.push(ColumnPredicate::ColumnCompare {
                            left_column_idx: left_idx,
                            op: compare_op,
                            right_column_idx: right_idx,
                        });
                    }
                    return Some(());
                }
            }

            // Skip other unsupported expressions
            Some(())
        }

        // BETWEEN: column BETWEEN low AND high
        // Only support ASYMMETRIC (default) BETWEEN for columnar optimization
        Expression::Between { expr: inner, low, high, negated: false, symmetric: false } => {
            if let Expression::ColumnRef(col_id) = inner.as_ref() {
                let table = col_id.table_canonical();
                let column = col_id.column_canonical();
                // Try to fold bounds to literals (handles arithmetic expressions like 1+2)
                if let (Some(low_val), Some(high_val)) =
                    (try_fold_constant(low), try_fold_constant(high))
                {
                    // Skip if column not in schema (cross-table predicate)
                    if let Some(column_idx) = schema.get_column_index(table, column) {
                        predicates.push(ColumnPredicate::Between {
                            column_idx,
                            low: low_val,
                            high: high_val,
                        });
                    }
                    return Some(());
                }
            }
            // Skip non-column BETWEEN expressions
            Some(())
        }

        // LIKE: column LIKE pattern
        Expression::Like { expr: inner, pattern, negated, escape } => {
            if let Expression::ColumnRef(col_id) = inner.as_ref() {
                let table = col_id.table_canonical();
                let column = col_id.column_canonical();
                // Extract pattern string from literal
                if let Expression::Literal(SqlValue::Character(pattern_str))
                | Expression::Literal(SqlValue::Varchar(pattern_str)) = pattern.as_ref()
                {
                    // Extract escape character from ESCAPE clause if present
                    let escape_char = escape.as_ref().and_then(|esc_expr| {
                        if let Expression::Literal(SqlValue::Character(s))
                        | Expression::Literal(SqlValue::Varchar(s)) = esc_expr.as_ref()
                        {
                            s.chars().next()
                        } else {
                            None
                        }
                    });

                    // Skip if column not in schema (cross-table predicate)
                    if let Some(column_idx) = schema.get_column_index(table, column) {
                        predicates.push(ColumnPredicate::Like {
                            column_idx,
                            pattern: pattern_str.to_string(),
                            negated: *negated,
                            case_sensitive: case_sensitive_like,
                            escape: escape_char,
                        });
                    }
                    return Some(());
                }
            }
            // Skip non-column LIKE expressions
            Some(())
        }

        // IN list: column IN (value1, value2, ...)
        Expression::InList { expr: inner, values, negated } => {
            if let Expression::ColumnRef(col_id) = inner.as_ref() {
                let table = col_id.table_canonical();
                let column = col_id.column_canonical();
                // Extract all values from the IN list (try to fold each to a constant)
                let mut folded_values = Vec::with_capacity(values.len());
                for value_expr in values {
                    if let Some(val) = try_fold_constant(value_expr) {
                        folded_values.push(val);
                    } else {
                        // Non-foldable value in IN list - can't optimize
                        return Some(());
                    }
                }

                if folded_values.is_empty() {
                    return Some(());
                }

                // Skip if column not in schema (cross-table predicate)
                if let Some(column_idx) = schema.get_column_index(table, column) {
                    // Determine if we should use strict type ordering (no coercion).
                    // SQLite IN expressions don't coerce strings for NONE or INTEGER affinity.
                    let use_strict_type_ordering = schema
                        .get_column_affinity(table, column)
                        .map(|affinity| {
                            matches!(affinity, TypeAffinity::None | TypeAffinity::Integer)
                        })
                        .unwrap_or(true);

                    predicates.push(ColumnPredicate::InList {
                        column_idx,
                        values: folded_values,
                        negated: *negated,
                        use_strict_type_ordering,
                    });
                }
                return Some(());
            }
            // Skip non-column IN expressions
            Some(())
        }

        // Skip any other expression types - don't fail
        _ => Some(()),
    }
}

#[cfg(test)]
mod tests {
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::DataType;

    use super::*;
    use crate::schema::CombinedSchema;

    fn create_test_schema() -> CombinedSchema {
        let schema = TableSchema::new(
            "test".to_string(),
            vec![
                ColumnSchema::new("col0".to_string(), DataType::Integer, false),
                ColumnSchema::new("col1".to_string(), DataType::Integer, false),
            ],
        );
        CombinedSchema::from_table("test".to_string(), schema)
    }

    #[test]
    fn test_try_fold_constant_literal() {
        let expr = Expression::Literal(SqlValue::Integer(42));
        assert_eq!(try_fold_constant(&expr), Some(SqlValue::Integer(42)));
    }

    #[test]
    fn test_try_fold_constant_addition() {
        // 1 + 2 should fold to 3
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::Literal(SqlValue::Integer(1))),
            op: BinaryOperator::Plus,
            right: Box::new(Expression::Literal(SqlValue::Integer(2))),
        };
        assert_eq!(try_fold_constant(&expr), Some(SqlValue::Integer(3)));
    }

    #[test]
    fn test_try_fold_constant_nested_arithmetic() {
        // (1 + 2) * 3 should fold to 9
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::Literal(SqlValue::Integer(1))),
                op: BinaryOperator::Plus,
                right: Box::new(Expression::Literal(SqlValue::Integer(2))),
            }),
            op: BinaryOperator::Multiply,
            right: Box::new(Expression::Literal(SqlValue::Integer(3))),
        };
        assert_eq!(try_fold_constant(&expr), Some(SqlValue::Integer(9)));
    }

    #[test]
    fn test_try_fold_constant_unary_minus() {
        // -5 should fold to -5
        let expr = Expression::UnaryOp {
            op: UnaryOperator::Minus,
            expr: Box::new(Expression::Literal(SqlValue::Integer(5))),
        };
        assert_eq!(try_fold_constant(&expr), Some(SqlValue::Integer(-5)));
    }

    #[test]
    fn test_try_fold_constant_column_ref_returns_none() {
        // Column references cannot be folded
        let expr = Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("x", false));
        assert_eq!(try_fold_constant(&expr), None);
    }

    #[test]
    fn test_between_with_arithmetic_bounds() {
        let schema = create_test_schema();

        // col0 BETWEEN 1 AND 1+2
        let expr = Expression::Between {
            expr: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                "col0", false,
            ))),
            low: Box::new(Expression::Literal(SqlValue::Integer(1))),
            high: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::Literal(SqlValue::Integer(1))),
                op: BinaryOperator::Plus,
                right: Box::new(Expression::Literal(SqlValue::Integer(2))),
            }),
            negated: false,
            symmetric: false,
        };

        let tree = extract_predicate_tree(&expr, &schema);
        assert!(tree.is_some());

        match tree.unwrap() {
            PredicateTree::Leaf(ColumnPredicate::Between { column_idx, low, high }) => {
                assert_eq!(column_idx, 0);
                assert_eq!(low, SqlValue::Integer(1));
                assert_eq!(high, SqlValue::Integer(3)); // 1+2 folded to 3
            }
            _ => panic!("Expected Between predicate"),
        }
    }

    #[test]
    fn test_comparison_with_arithmetic_value() {
        let schema = create_test_schema();

        // col0 < 10 - 3
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                "col0", false,
            ))),
            op: BinaryOperator::LessThan,
            right: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::Literal(SqlValue::Integer(10))),
                op: BinaryOperator::Minus,
                right: Box::new(Expression::Literal(SqlValue::Integer(3))),
            }),
        };

        let tree = extract_predicate_tree(&expr, &schema);
        assert!(tree.is_some());

        match tree.unwrap() {
            PredicateTree::Leaf(ColumnPredicate::LessThan { column_idx, value }) => {
                assert_eq!(column_idx, 0);
                assert_eq!(value, SqlValue::Integer(7)); // 10-3 folded to 7
            }
            _ => panic!("Expected LessThan predicate"),
        }
    }

    #[test]
    fn test_reverse_comparison_with_arithmetic() {
        let schema = create_test_schema();

        // 2*5 > col0 should become col0 < 10
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::Literal(SqlValue::Integer(2))),
                op: BinaryOperator::Multiply,
                right: Box::new(Expression::Literal(SqlValue::Integer(5))),
            }),
            op: BinaryOperator::GreaterThan,
            right: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                "col0", false,
            ))),
        };

        let tree = extract_predicate_tree(&expr, &schema);
        assert!(tree.is_some());

        match tree.unwrap() {
            PredicateTree::Leaf(ColumnPredicate::LessThan { column_idx, value }) => {
                assert_eq!(column_idx, 0);
                assert_eq!(value, SqlValue::Integer(10)); // 2*5 folded to 10
            }
            _ => panic!("Expected LessThan predicate (reversed from GreaterThan)"),
        }
    }

    #[test]
    fn test_in_list_with_arithmetic_values() {
        let schema = create_test_schema();

        // col0 IN (1, 1+1, 2+1)
        let expr = Expression::InList {
            expr: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                "col0", false,
            ))),
            values: vec![
                Expression::Literal(SqlValue::Integer(1)),
                Expression::BinaryOp {
                    left: Box::new(Expression::Literal(SqlValue::Integer(1))),
                    op: BinaryOperator::Plus,
                    right: Box::new(Expression::Literal(SqlValue::Integer(1))),
                },
                Expression::BinaryOp {
                    left: Box::new(Expression::Literal(SqlValue::Integer(2))),
                    op: BinaryOperator::Plus,
                    right: Box::new(Expression::Literal(SqlValue::Integer(1))),
                },
            ],
            negated: false,
        };

        let tree = extract_predicate_tree(&expr, &schema);
        assert!(tree.is_some());

        match tree.unwrap() {
            PredicateTree::Leaf(ColumnPredicate::InList {
                column_idx, values, negated, ..
            }) => {
                assert_eq!(column_idx, 0);
                assert!(!negated);
                assert_eq!(values.len(), 3);
                assert_eq!(values[0], SqlValue::Integer(1));
                assert_eq!(values[1], SqlValue::Integer(2)); // 1+1 folded to 2
                assert_eq!(values[2], SqlValue::Integer(3)); // 2+1 folded to 3
            }
            _ => panic!("Expected InList predicate"),
        }
    }

    #[test]
    fn test_between_with_column_bound_returns_none() {
        let schema = create_test_schema();

        // col0 BETWEEN 1 AND col1 (col1 cannot be folded)
        let expr = Expression::Between {
            expr: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                "col0", false,
            ))),
            low: Box::new(Expression::Literal(SqlValue::Integer(1))),
            high: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                "col1", false,
            ))),
            negated: false,
            symmetric: false,
        };

        let tree = extract_predicate_tree(&expr, &schema);
        assert!(tree.is_none());
    }

    #[test]
    fn test_column_to_column_comparison() {
        let schema = create_test_schema();

        // col0 < col1 (column-to-column comparison)
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                "col0", false,
            ))),
            op: BinaryOperator::LessThan,
            right: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                "col1", false,
            ))),
        };

        let tree = extract_predicate_tree(&expr, &schema);
        assert!(tree.is_some());

        match tree.unwrap() {
            PredicateTree::Leaf(ColumnPredicate::ColumnCompare {
                left_column_idx,
                op,
                right_column_idx,
            }) => {
                assert_eq!(left_column_idx, 0);
                assert_eq!(op, CompareOp::LessThan);
                assert_eq!(right_column_idx, 1);
            }
            _ => panic!("Expected ColumnCompare predicate"),
        }
    }

    #[test]
    fn test_column_to_column_all_operators() {
        let schema = create_test_schema();

        let operators = [
            (BinaryOperator::LessThan, CompareOp::LessThan),
            (BinaryOperator::GreaterThan, CompareOp::GreaterThan),
            (BinaryOperator::LessThanOrEqual, CompareOp::LessThanOrEqual),
            (BinaryOperator::GreaterThanOrEqual, CompareOp::GreaterThanOrEqual),
            (BinaryOperator::Equal, CompareOp::Equal),
            (BinaryOperator::NotEqual, CompareOp::NotEqual),
        ];

        for (binary_op, expected_compare_op) in operators {
            let expr = Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                    "col0", false,
                ))),
                op: binary_op,
                right: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                    "col1", false,
                ))),
            };

            let tree = extract_predicate_tree(&expr, &schema);
            assert!(tree.is_some(), "Should extract predicate for operator {:?}", binary_op);

            match tree.unwrap() {
                PredicateTree::Leaf(ColumnPredicate::ColumnCompare { op, .. }) => {
                    assert_eq!(op, expected_compare_op, "Operator mismatch for {:?}", binary_op);
                }
                _ => panic!("Expected ColumnCompare predicate for {:?}", binary_op),
            }
        }
    }

    #[test]
    fn test_column_to_column_legacy_path() {
        let schema = create_test_schema();

        // col0 < col1 AND col0 > 5 (mix of column-to-column and column-to-value)
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                    "col0", false,
                ))),
                op: BinaryOperator::LessThan,
                right: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                    "col1", false,
                ))),
            }),
            op: BinaryOperator::And,
            right: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                    "col0", false,
                ))),
                op: BinaryOperator::GreaterThan,
                right: Box::new(Expression::Literal(SqlValue::Integer(5))),
            }),
        };

        let predicates = extract_column_predicates(&expr, &schema, false);
        assert!(predicates.is_some());

        let predicates = predicates.unwrap();
        assert_eq!(predicates.len(), 2);

        // First predicate should be column-to-column
        match &predicates[0] {
            ColumnPredicate::ColumnCompare { left_column_idx, op, right_column_idx } => {
                assert_eq!(*left_column_idx, 0);
                assert_eq!(*op, CompareOp::LessThan);
                assert_eq!(*right_column_idx, 1);
            }
            _ => panic!("Expected ColumnCompare predicate"),
        }

        // Second predicate should be column-to-value
        match &predicates[1] {
            ColumnPredicate::GreaterThan { column_idx, value } => {
                assert_eq!(*column_idx, 0);
                assert_eq!(*value, SqlValue::Integer(5));
            }
            _ => panic!("Expected GreaterThan predicate"),
        }
    }
}
