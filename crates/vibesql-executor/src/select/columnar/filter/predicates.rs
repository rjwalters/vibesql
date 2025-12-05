use crate::schema::CombinedSchema;
use vibesql_ast::{BinaryOperator, Expression};
use vibesql_types::SqlValue;

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
    Like { column_idx: usize, pattern: String, negated: bool },

    /// column IN (value1, value2, ...)
    InList { column_idx: usize, values: Vec<SqlValue>, negated: bool },
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
) -> Option<Vec<ColumnPredicate>> {
    let mut predicates = Vec::new();
    extract_predicates_recursive(expr, schema, &mut predicates)?;
    // Return None if no predicates were extracted (all were cross-table or unsupported)
    // This allows fallback to generic predicate evaluation
    if predicates.is_empty() {
        None
    } else {
        Some(predicates)
    }
}

/// Recursively extract predicates as a tree from an expression (handles OR)
fn extract_tree_recursive(expr: &Expression, schema: &CombinedSchema) -> Option<PredicateTree> {
    match expr {
        // AND: combine both sides
        Expression::BinaryOp { left, op: BinaryOperator::And, right } => {
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
        Expression::BinaryOp { left, op: BinaryOperator::Or, right } => {
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
                    BinaryOperator::LessThan => {
                        ColumnPredicate::LessThan { column_idx, value: value.clone() }
                    }
                    BinaryOperator::GreaterThan => {
                        ColumnPredicate::GreaterThan { column_idx, value: value.clone() }
                    }
                    BinaryOperator::LessThanOrEqual => {
                        ColumnPredicate::LessThanOrEqual { column_idx, value: value.clone() }
                    }
                    BinaryOperator::GreaterThanOrEqual => {
                        ColumnPredicate::GreaterThanOrEqual { column_idx, value: value.clone() }
                    }
                    BinaryOperator::Equal => {
                        ColumnPredicate::Equal { column_idx, value: value.clone() }
                    }
                    BinaryOperator::NotEqual => {
                        ColumnPredicate::NotEqual { column_idx, value: value.clone() }
                    }
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
                    BinaryOperator::LessThan => {
                        ColumnPredicate::GreaterThan { column_idx, value: value.clone() }
                    }
                    BinaryOperator::GreaterThan => {
                        ColumnPredicate::LessThan { column_idx, value: value.clone() }
                    }
                    BinaryOperator::LessThanOrEqual => {
                        ColumnPredicate::GreaterThanOrEqual { column_idx, value: value.clone() }
                    }
                    BinaryOperator::GreaterThanOrEqual => {
                        ColumnPredicate::LessThanOrEqual { column_idx, value: value.clone() }
                    }
                    BinaryOperator::Equal => {
                        ColumnPredicate::Equal { column_idx, value: value.clone() }
                    }
                    // NotEqual is symmetric: literal <> column == column <> literal
                    BinaryOperator::NotEqual => {
                        ColumnPredicate::NotEqual { column_idx, value: value.clone() }
                    }
                    _ => return None,
                };
                return Some(PredicateTree::Leaf(predicate));
            }

            None
        }

        // BETWEEN: column BETWEEN low AND high
        // Only support ASYMMETRIC (default) BETWEEN for columnar optimization
        // SYMMETRIC BETWEEN falls through to general evaluator which handles bounds swapping
        Expression::Between { expr: inner, low, high, negated: false, symmetric: false } => {
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

        // LIKE: column LIKE pattern
        Expression::Like { expr: inner, pattern, negated, .. } => {
            if let Expression::ColumnRef { table, column } = inner.as_ref() {
                // Extract pattern string from literal
                if let Expression::Literal(SqlValue::Character(pattern_str))
                | Expression::Literal(SqlValue::Varchar(pattern_str)) = pattern.as_ref()
                {
                    let column_idx = schema.get_column_index(table.as_deref(), column)?;
                    return Some(PredicateTree::Leaf(ColumnPredicate::Like {
                        column_idx,
                        pattern: pattern_str.clone(),
                        negated: *negated,
                    }));
                }
            }
            None
        }

        // IN list: column IN (value1, value2, ...)
        Expression::InList { expr: inner, values, negated } => {
            if let Expression::ColumnRef { table, column } = inner.as_ref() {
                // Extract all literal values from the IN list
                let mut literal_values = Vec::with_capacity(values.len());
                for value_expr in values {
                    if let Expression::Literal(val) = value_expr {
                        literal_values.push(val.clone());
                    } else {
                        // Non-literal value in IN list - can't optimize
                        return None;
                    }
                }

                if literal_values.is_empty() {
                    return None;
                }

                let column_idx = schema.get_column_index(table.as_deref(), column)?;
                return Some(PredicateTree::Leaf(ColumnPredicate::InList {
                    column_idx,
                    values: literal_values,
                    negated: *negated,
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
) -> Option<()> {
    match expr {
        // AND: extract predicates from both sides
        // Important: Don't fail if one side can't be extracted - just skip that predicate
        // This allows Q3-style queries where WHERE has both table-local and cross-table predicates
        Expression::BinaryOp { left, op: BinaryOperator::And, right } => {
            // Try both sides - don't propagate failure from either side
            let _ = extract_predicates_recursive(left, schema, predicates);
            let _ = extract_predicates_recursive(right, schema, predicates);
            Some(())
        }

        // Binary comparison: column op literal
        Expression::BinaryOp { left, op, right } => {
            // Try: column op literal
            if let (Expression::ColumnRef { table, column }, Expression::Literal(value)) =
                (left.as_ref(), right.as_ref())
            {
                // Skip if column not in schema (cross-table predicate)
                if let Some(column_idx) = schema.get_column_index(table.as_deref(), column) {
                    let predicate = match op {
                        BinaryOperator::LessThan => {
                            ColumnPredicate::LessThan { column_idx, value: value.clone() }
                        }
                        BinaryOperator::GreaterThan => {
                            ColumnPredicate::GreaterThan { column_idx, value: value.clone() }
                        }
                        BinaryOperator::LessThanOrEqual => {
                            ColumnPredicate::LessThanOrEqual { column_idx, value: value.clone() }
                        }
                        BinaryOperator::GreaterThanOrEqual => {
                            ColumnPredicate::GreaterThanOrEqual { column_idx, value: value.clone() }
                        }
                        BinaryOperator::Equal => {
                            ColumnPredicate::Equal { column_idx, value: value.clone() }
                        }
                        BinaryOperator::NotEqual => {
                            ColumnPredicate::NotEqual { column_idx, value: value.clone() }
                        }
                        _ => return Some(()), // Skip unsupported operator
                    };
                    predicates.push(predicate);
                }
                return Some(());
            }

            // Try: literal op column (reverse the comparison)
            if let (Expression::Literal(value), Expression::ColumnRef { table, column }) =
                (left.as_ref(), right.as_ref())
            {
                // Skip if column not in schema (cross-table predicate)
                if let Some(column_idx) = schema.get_column_index(table.as_deref(), column) {
                    let predicate = match op {
                        // Reverse the comparison: literal < column => column > literal
                        BinaryOperator::LessThan => {
                            ColumnPredicate::GreaterThan { column_idx, value: value.clone() }
                        }
                        BinaryOperator::GreaterThan => {
                            ColumnPredicate::LessThan { column_idx, value: value.clone() }
                        }
                        BinaryOperator::LessThanOrEqual => {
                            ColumnPredicate::GreaterThanOrEqual { column_idx, value: value.clone() }
                        }
                        BinaryOperator::GreaterThanOrEqual => {
                            ColumnPredicate::LessThanOrEqual { column_idx, value: value.clone() }
                        }
                        BinaryOperator::Equal => {
                            ColumnPredicate::Equal { column_idx, value: value.clone() }
                        }
                        // NotEqual is symmetric: literal <> column == column <> literal
                        BinaryOperator::NotEqual => {
                            ColumnPredicate::NotEqual { column_idx, value: value.clone() }
                        }
                        _ => return Some(()), // Skip unsupported operator
                    };
                    predicates.push(predicate);
                }
                return Some(());
            }

            // Skip cross-table predicates (column op column) - not a failure
            Some(())
        }

        // BETWEEN: column BETWEEN low AND high
        // Only support ASYMMETRIC (default) BETWEEN for columnar optimization
        Expression::Between { expr: inner, low, high, negated: false, symmetric: false } => {
            if let Expression::ColumnRef { table, column } = inner.as_ref() {
                if let (Expression::Literal(low_val), Expression::Literal(high_val)) =
                    (low.as_ref(), high.as_ref())
                {
                    // Skip if column not in schema (cross-table predicate)
                    if let Some(column_idx) = schema.get_column_index(table.as_deref(), column) {
                        predicates.push(ColumnPredicate::Between {
                            column_idx,
                            low: low_val.clone(),
                            high: high_val.clone(),
                        });
                    }
                    return Some(());
                }
            }
            // Skip non-column BETWEEN expressions
            Some(())
        }

        // LIKE: column LIKE pattern
        Expression::Like { expr: inner, pattern, negated, .. } => {
            if let Expression::ColumnRef { table, column } = inner.as_ref() {
                // Extract pattern string from literal
                if let Expression::Literal(SqlValue::Character(pattern_str))
                | Expression::Literal(SqlValue::Varchar(pattern_str)) = pattern.as_ref()
                {
                    // Skip if column not in schema (cross-table predicate)
                    if let Some(column_idx) = schema.get_column_index(table.as_deref(), column) {
                        predicates.push(ColumnPredicate::Like {
                            column_idx,
                            pattern: pattern_str.clone(),
                            negated: *negated,
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
            if let Expression::ColumnRef { table, column } = inner.as_ref() {
                // Extract all literal values from the IN list
                let mut literal_values = Vec::with_capacity(values.len());
                for value_expr in values {
                    if let Expression::Literal(val) = value_expr {
                        literal_values.push(val.clone());
                    } else {
                        // Non-literal value in IN list - can't optimize
                        return Some(());
                    }
                }

                if literal_values.is_empty() {
                    return Some(());
                }

                // Skip if column not in schema (cross-table predicate)
                if let Some(column_idx) = schema.get_column_index(table.as_deref(), column) {
                    predicates.push(ColumnPredicate::InList {
                        column_idx,
                        values: literal_values,
                        negated: *negated,
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
