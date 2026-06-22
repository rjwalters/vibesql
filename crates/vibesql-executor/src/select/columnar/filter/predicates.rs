use vibesql_ast::{BinaryOperator, Expression, UnaryOperator};
use vibesql_types::{DataType, SqlMode, SqlValue, TypeAffinity};

use super::comparison::parse_date_string;
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
pub fn extract_predicate_tree(
    expr: &Expression,
    schema: &CombinedSchema,
    case_sensitive_like: bool,
) -> Option<PredicateTree> {
    let tree = extract_tree_recursive(expr, schema, case_sensitive_like)?;
    // Issue #5335: decline pushdown entirely when any extracted predicate
    // pairs a column with a literal the columnar comparators cannot evaluate
    // faithfully (e.g. DATE vs unparseable string must raise the evaluator's
    // type-mismatch error; temporal vs numeric has no columnar ordering).
    // Declining must be all-or-nothing: callers mark the WHERE clause as
    // consumed after columnar filtering, so a silently skipped predicate
    // would over-return rows.
    if !tree_supported_by_columnar(&tree, schema) {
        return None;
    }
    Some(tree)
}

/// Check whether every leaf of a predicate tree can be evaluated faithfully
/// by the columnar comparators (see `predicate_supported_by_columnar`).
fn tree_supported_by_columnar(tree: &PredicateTree, schema: &CombinedSchema) -> bool {
    match tree {
        PredicateTree::And(children) | PredicateTree::Or(children) => {
            children.iter().all(|child| tree_supported_by_columnar(child, schema))
        }
        PredicateTree::Leaf(predicate) => predicate_supported_by_columnar(predicate, schema),
    }
}

/// Check whether a single column predicate can be evaluated faithfully by the
/// columnar comparators (`filter::comparison::compare_values` and the SIMD
/// kernels), given the column's declared data type.
///
/// Issue #5335: the columnar comparators implement the #5329 semantics for
/// temporal columns versus matching temporal literals and strings, but they
/// have no error channel (DATE vs unparseable string must raise a
/// type-mismatch error in the expression evaluator) and no defined ordering
/// for mixed temporal/non-temporal pairs. Those combinations must fall back
/// to the full expression evaluator, so extraction declines them here.
fn predicate_supported_by_columnar(predicate: &ColumnPredicate, schema: &CombinedSchema) -> bool {
    match predicate {
        ColumnPredicate::LessThan { column_idx, value }
        | ColumnPredicate::GreaterThan { column_idx, value }
        | ColumnPredicate::GreaterThanOrEqual { column_idx, value }
        | ColumnPredicate::LessThanOrEqual { column_idx, value }
        | ColumnPredicate::Equal { column_idx, value }
        | ColumnPredicate::NotEqual { column_idx, value } => {
            value_supported_for_column(schema.get_column_type_by_index(*column_idx), value)
        }
        ColumnPredicate::Between { column_idx, low, high } => {
            let col_type = schema.get_column_type_by_index(*column_idx);
            value_supported_for_column(col_type, low) && value_supported_for_column(col_type, high)
        }
        ColumnPredicate::InList { column_idx, values, .. } => {
            let col_type = schema.get_column_type_by_index(*column_idx);
            values.iter().all(|v| value_supported_for_column(col_type, v))
        }
        // LIKE patterns are strings; existing comparator behavior applies
        ColumnPredicate::Like { .. } => true,
        ColumnPredicate::ColumnCompare { left_column_idx, right_column_idx, .. } => {
            column_compare_supported(
                schema.get_column_type_by_index(*left_column_idx),
                schema.get_column_type_by_index(*right_column_idx),
            )
        }
    }
}

/// Whether a column's declared type is a temporal type
fn is_temporal_type(t: &DataType) -> bool {
    matches!(t, DataType::Date | DataType::Time { .. } | DataType::Timestamp { .. })
}

/// Whether a column's declared type is a character string type
fn is_string_type(t: &DataType) -> bool {
    matches!(
        t,
        DataType::Character { .. }
            | DataType::Varchar { .. }
            | DataType::CharacterLargeObject
            | DataType::Name
    )
}

/// Whether a column's declared type is the binary (BLOB) type
fn is_blob_type(t: &DataType) -> bool {
    matches!(t, DataType::BinaryLargeObject)
}

/// Whether a literal value is one of the numeric types the columnar
/// comparator (`compare_values`) has faithful arms for.
fn is_numeric_value(v: &SqlValue) -> bool {
    matches!(
        v,
        SqlValue::Integer(_)
            | SqlValue::Bigint(_)
            | SqlValue::Smallint(_)
            | SqlValue::Float(_)
            | SqlValue::Double(_)
            | SqlValue::Real(_)
            | SqlValue::Numeric(_)
    )
}

/// Whether a string would be coerced to a number by the expression
/// evaluator's NUMERIC-affinity rules (`try_coerce_string_to_numeric`).
/// Temporal columns have NUMERIC affinity, so numeric-parseable string
/// literals against them become Timestamp/Date/Time-vs-number comparisons in
/// the evaluator (which yield false), not TEXT-rendering comparisons.
fn string_coerces_to_numeric(s: &str) -> bool {
    let trimmed = s.trim();
    trimmed.parse::<i64>().is_ok() || trimmed.parse::<f64>().is_ok()
}

/// Check whether a literal operand can be compared faithfully against a
/// column of the given declared type by the columnar comparators.
fn value_supported_for_column(col_type: Option<&DataType>, value: &SqlValue) -> bool {
    // NULL literals are handled uniformly (comparison is UNKNOWN)
    if matches!(value, SqlValue::Null) {
        return true;
    }

    match col_type {
        Some(DataType::Date) => match value {
            SqlValue::Date(_) => true,
            // Date vs parseable string compares parse-first (hot path for
            // TPC-H date range predicates); unparseable strings must raise
            // the evaluator's type-mismatch error, so decline pushdown.
            // (Numeric-looking strings fail YYYY-MM-DD parsing and are
            // declined too, matching the evaluator's affinity coercion.)
            SqlValue::Varchar(s) | SqlValue::Character(s) => parse_date_string(s).is_some(),
            _ => false,
        },
        Some(DataType::Timestamp { .. }) => match value {
            // Timestamp vs Timestamp compares temporally; Timestamp vs a
            // non-numeric string compares TEXT renderings (#5329).
            // Numeric-parseable strings are coerced to numbers by the
            // evaluator's NUMERIC-affinity rules first (temporal vs numeric
            // is then always false), so decline those to preserve parity.
            SqlValue::Timestamp(_) => true,
            SqlValue::Varchar(s) | SqlValue::Character(s) => !string_coerces_to_numeric(s),
            _ => false,
        },
        Some(DataType::Time { .. }) => match value {
            SqlValue::Time(_) => true,
            SqlValue::Varchar(s) | SqlValue::Character(s) => !string_coerces_to_numeric(s),
            _ => false,
        },
        // Issue #5340: the expression evaluator raises a type-mismatch error
        // for Boolean vs string/BLOB/temporal operands, and the columnar
        // comparator has no error channel, so decline those. Boolean vs
        // Boolean and Boolean vs numeric compare faithfully in both paths
        // (booleans coerce to 0/1).
        Some(DataType::Boolean) => matches!(value, SqlValue::Boolean(_)) || is_numeric_value(value),
        Some(other) => match value {
            // Temporal literal against a non-temporal column: only string
            // columns have comparator support (parse-first for Date, TEXT
            // rendering for Timestamp/Time).
            SqlValue::Date(_) | SqlValue::Timestamp(_) | SqlValue::Time(_) => is_string_type(other),
            // Issue #5340: Blob literals are only supported against BLOB
            // columns (bytewise comparison). Against numeric/string columns
            // the evaluator applies storage-class ordering (numeric < TEXT <
            // BLOB), but the numeric/string SIMD kernels have no blob arm
            // (they raise ColumnarTypeMismatch), so decline and let the
            // evaluator handle it.
            SqlValue::Blob(_) => is_blob_type(other),
            // Issue #5340: Boolean literal vs string/BLOB column raises a
            // type-mismatch error in the evaluator; decline (no error
            // channel in the columnar path).
            SqlValue::Boolean(_) => !is_string_type(other) && !is_blob_type(other),
            // Everything else (including string/numeric literals against
            // BLOB columns, which compare_values orders via the #5340
            // storage-class arms) keeps existing comparator behavior.
            _ => true,
        },
        // Unknown column type (e.g. outer-scope reference): allow existing
        // behavior except for temporal and Blob literals, where we cannot
        // prove the comparators have a faithful arm.
        None => !matches!(
            value,
            SqlValue::Date(_) | SqlValue::Timestamp(_) | SqlValue::Time(_) | SqlValue::Blob(_)
        ),
    }
}

/// Check whether a column-to-column comparison is supported by the columnar
/// comparators given both columns' declared types.
fn column_compare_supported(left: Option<&DataType>, right: Option<&DataType>) -> bool {
    match (left, right) {
        (Some(l), Some(r)) => {
            let l_temporal = is_temporal_type(l);
            let r_temporal = is_temporal_type(r);
            if l_temporal && r_temporal {
                // Same temporal kind compares natively; mixed kinds (e.g.
                // Date vs Timestamp) have no columnar ordering.
                std::mem::discriminant(l) == std::mem::discriminant(r)
            } else if l_temporal || r_temporal {
                // Temporal vs non-temporal column: the evaluator applies
                // per-row affinity coercion (numeric-looking strings in a
                // TEXT column coerce to numbers against a NUMERIC-affinity
                // temporal column), which the columnar comparator cannot
                // replicate. Decline so the evaluator runs.
                false
            } else {
                true
            }
        }
        // Unknown types: keep existing behavior
        _ => true,
    }
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
        return None;
    }
    // Issue #5335: decline pushdown entirely when any extracted predicate
    // pairs a column with a literal the columnar comparators cannot evaluate
    // faithfully. Must be all-or-nothing: callers mark the WHERE clause as
    // consumed after columnar filtering, so skipping one predicate here
    // would over-return rows.
    if !predicates.iter().all(|p| predicate_supported_by_columnar(p, schema)) {
        return None;
    }
    Some(predicates)
}

/// Extract a flat list of AND-ed column predicates that *fully* cover the WHERE
/// clause, or `None` if any conjunct cannot be represented columnarly.
///
/// Issue #5719: `extract_column_predicates` is deliberately lenient — its AND
/// branch silently skips conjuncts it cannot fold (so cross-table push-down
/// callers can still extract the columns they own). That leniency is *wrong*
/// for the columnar pipeline `apply_filter` step, which marks the WHERE clause
/// as fully consumed after SIMD filtering. When a WHERE mixes a foldable
/// predicate (`status = 'failed'`) with a non-foldable one
/// (`run_id = (SELECT MAX(run_id) FROM t)`), the lenient extractor returns a
/// partial set, the SIMD path applies only the partial predicate, and the
/// scalar-subquery predicate is dropped → over-counted results.
///
/// This function is strict and all-or-nothing: it builds the `PredicateTree`
/// (whose AND branch uses `?`, so it fails when *any* conjunct is
/// non-extractable) and flattens it to a flat AND-of-leaves list suitable for
/// `simd_filter_batch`. It returns `None` when:
/// - any conjunct cannot be extracted (→ caller must fall back to the full expression evaluator,
///   which handles scalar subqueries, functions, etc.);
/// - the tree contains an OR node (the flat SIMD filter list cannot represent disjunctions; such
///   queries already fall back today).
///
/// When it returns `Some`, the returned predicates fully cover the WHERE
/// clause, so the fast SIMD path stays correct *and* there is no perf
/// regression for analytical queries whose WHERE is fully columnar.
pub fn extract_full_coverage_predicates(
    expr: &Expression,
    schema: &CombinedSchema,
    case_sensitive_like: bool,
) -> Option<Vec<ColumnPredicate>> {
    let tree = extract_predicate_tree(expr, schema, case_sensitive_like)?;
    let mut predicates = Vec::new();
    flatten_and_tree(&tree, &mut predicates)?;
    if predicates.is_empty() {
        return None;
    }
    Some(predicates)
}

/// Flatten a strictly-AND predicate tree into a flat list of leaf predicates.
///
/// Returns `None` if the tree contains an OR node, since a flat AND-only
/// predicate list cannot represent disjunctions.
fn flatten_and_tree(tree: &PredicateTree, out: &mut Vec<ColumnPredicate>) -> Option<()> {
    match tree {
        PredicateTree::Leaf(predicate) => {
            out.push(predicate.clone());
            Some(())
        }
        PredicateTree::And(children) => {
            for child in children {
                flatten_and_tree(child, out)?;
            }
            Some(())
        }
        PredicateTree::Or(_) => None,
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

    fn create_temporal_schema() -> CombinedSchema {
        let schema = TableSchema::new(
            "t".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new(
                    "ts".to_string(),
                    DataType::Timestamp { with_timezone: false },
                    true,
                ),
                ColumnSchema::new("d".to_string(), DataType::Date, true),
            ],
        );
        CombinedSchema::from_table("t".to_string(), schema)
    }

    fn comparison_expr(column: &str, op: BinaryOperator, value: SqlValue) -> Expression {
        Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                column, false,
            ))),
            op,
            right: Box::new(Expression::Literal(value)),
        }
    }

    /// Issue #5335: extraction supports Timestamp columns vs Timestamp
    /// literals and non-numeric strings (the comparators implement #5329
    /// semantics for those).
    #[test]
    fn test_timestamp_column_supported_literals_extracted() {
        use std::str::FromStr;
        let schema = create_temporal_schema();

        let ts_literal =
            SqlValue::Timestamp(vibesql_types::Timestamp::from_str("2017-07-20 15:30:00").unwrap());
        for value in [ts_literal, SqlValue::Varchar(arcstr::ArcStr::from("2017-07-21"))] {
            let expr = comparison_expr("ts", BinaryOperator::GreaterThanOrEqual, value.clone());
            assert!(
                extract_column_predicates(&expr, &schema, false).is_some(),
                "expected pushdown for ts >= {value:?}"
            );
            assert!(extract_predicate_tree(&expr, &schema, false).is_some());
        }
    }

    /// Issue #5335: extraction declines combinations the columnar comparators
    /// cannot evaluate faithfully (must be all-or-nothing so the WHERE clause
    /// is not marked consumed with predicates silently dropped).
    #[test]
    fn test_unsupported_temporal_literals_decline_extraction() {
        let schema = create_temporal_schema();

        // Numeric-parseable string vs Timestamp column: the evaluator's
        // NUMERIC-affinity rules coerce it to a number first.
        let expr = comparison_expr(
            "ts",
            BinaryOperator::LessThan,
            SqlValue::Varchar(arcstr::ArcStr::from("1999")),
        );
        assert!(extract_column_predicates(&expr, &schema, false).is_none());
        assert!(extract_predicate_tree(&expr, &schema, false).is_none());

        // Numeric literal vs Timestamp column: no columnar ordering.
        let expr = comparison_expr("ts", BinaryOperator::Equal, SqlValue::Integer(1999));
        assert!(extract_column_predicates(&expr, &schema, false).is_none());

        // Unparseable string vs Date column: the evaluator raises a
        // type-mismatch error, which the comparators cannot.
        let expr = comparison_expr(
            "d",
            BinaryOperator::Equal,
            SqlValue::Varchar(arcstr::ArcStr::from("not-a-date")),
        );
        assert!(extract_column_predicates(&expr, &schema, false).is_none());
        assert!(extract_predicate_tree(&expr, &schema, false).is_none());

        // AND with one unsupported predicate declines the whole extraction.
        let expr = Expression::BinaryOp {
            left: Box::new(comparison_expr(
                "id",
                BinaryOperator::GreaterThan,
                SqlValue::Integer(0),
            )),
            op: BinaryOperator::And,
            right: Box::new(comparison_expr(
                "ts",
                BinaryOperator::LessThan,
                SqlValue::Varchar(arcstr::ArcStr::from("1999")),
            )),
        };
        assert!(extract_column_predicates(&expr, &schema, false).is_none());
        assert!(extract_predicate_tree(&expr, &schema, false).is_none());
    }

    /// Issue #5335 perf guard: Date columns vs parseable strings stay on the
    /// columnar fast path (TPC-H date range predicates).
    #[test]
    fn test_date_column_parseable_string_still_extracted() {
        let schema = create_temporal_schema();
        let expr = comparison_expr(
            "d",
            BinaryOperator::GreaterThanOrEqual,
            SqlValue::Varchar(arcstr::ArcStr::from("1994-01-01")),
        );
        assert!(extract_column_predicates(&expr, &schema, false).is_some());
        assert!(extract_predicate_tree(&expr, &schema, false).is_some());
    }

    fn create_blob_bool_schema() -> CombinedSchema {
        let schema = TableSchema::new(
            "t".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new("b".to_string(), DataType::BinaryLargeObject, true),
                ColumnSchema::new("flag".to_string(), DataType::Boolean, true),
                ColumnSchema::new("s".to_string(), DataType::Varchar { max_length: None }, true),
            ],
        );
        CombinedSchema::from_table("t".to_string(), schema)
    }

    /// Issue #5340: Blob columns vs string/numeric literals stay on the
    /// columnar path (compare_values now implements the storage-class
    /// ordering numeric < TEXT < BLOB), and Blob vs Blob is bytewise.
    #[test]
    fn test_blob_column_supported_literals_extracted() {
        let schema = create_blob_bool_schema();
        for value in [
            SqlValue::Varchar(arcstr::ArcStr::from("abc")),
            SqlValue::Integer(5),
            SqlValue::Blob(vec![0x61, 0x62]),
        ] {
            let expr = comparison_expr("b", BinaryOperator::GreaterThanOrEqual, value.clone());
            assert!(
                extract_column_predicates(&expr, &schema, false).is_some(),
                "expected pushdown for b >= {value:?}"
            );
            assert!(extract_predicate_tree(&expr, &schema, false).is_some());
        }
    }

    /// Issue #5340: combinations where the evaluator raises a type-mismatch
    /// error (no columnar error channel) or the SIMD kernels have no blob arm
    /// must decline pushdown so the expression evaluator handles them.
    #[test]
    fn test_blob_boolean_unsupported_pairs_decline_extraction() {
        let schema = create_blob_bool_schema();

        // Boolean column vs string literal: evaluator raises TypeMismatch
        let expr = comparison_expr(
            "flag",
            BinaryOperator::Equal,
            SqlValue::Varchar(arcstr::ArcStr::from("true")),
        );
        assert!(extract_column_predicates(&expr, &schema, false).is_none());
        assert!(extract_predicate_tree(&expr, &schema, false).is_none());

        // Boolean column vs Blob literal: evaluator raises TypeMismatch
        let expr = comparison_expr("flag", BinaryOperator::Equal, SqlValue::Blob(vec![0x01]));
        assert!(extract_column_predicates(&expr, &schema, false).is_none());

        // Blob column vs Boolean literal: evaluator raises TypeMismatch
        let expr = comparison_expr("b", BinaryOperator::Equal, SqlValue::Boolean(true));
        assert!(extract_column_predicates(&expr, &schema, false).is_none());

        // String column vs Boolean literal: evaluator raises TypeMismatch
        let expr = comparison_expr("s", BinaryOperator::Equal, SqlValue::Boolean(true));
        assert!(extract_column_predicates(&expr, &schema, false).is_none());

        // Numeric/string column vs Blob literal: the numeric/string SIMD
        // kernels have no blob arm, so decline (evaluator orders these)
        let expr = comparison_expr("id", BinaryOperator::LessThan, SqlValue::Blob(vec![0x01]));
        assert!(extract_column_predicates(&expr, &schema, false).is_none());
        let expr = comparison_expr("s", BinaryOperator::LessThan, SqlValue::Blob(vec![0x01]));
        assert!(extract_column_predicates(&expr, &schema, false).is_none());
    }

    /// Issue #5340: Boolean columns keep pushdown for the pairs both paths
    /// evaluate faithfully (Boolean and numeric operands coerce to 0/1).
    #[test]
    fn test_boolean_column_supported_literals_extracted() {
        let schema = create_blob_bool_schema();
        for value in [SqlValue::Boolean(true), SqlValue::Integer(1)] {
            let expr = comparison_expr("flag", BinaryOperator::Equal, value.clone());
            assert!(
                extract_column_predicates(&expr, &schema, false).is_some(),
                "expected pushdown for flag = {value:?}"
            );
        }
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

        let tree = extract_predicate_tree(&expr, &schema, false);
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

        let tree = extract_predicate_tree(&expr, &schema, false);
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

        let tree = extract_predicate_tree(&expr, &schema, false);
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

        let tree = extract_predicate_tree(&expr, &schema, false);
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

        let tree = extract_predicate_tree(&expr, &schema, false);
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

        let tree = extract_predicate_tree(&expr, &schema, false);
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

            let tree = extract_predicate_tree(&expr, &schema, false);
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
