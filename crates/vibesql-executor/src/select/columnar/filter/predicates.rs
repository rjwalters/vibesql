use vibesql_ast::{BinaryOperator, Expression, UnaryOperator};
use vibesql_types::{DataType, SqlMode, SqlValue, TypeAffinity};

use super::comparison::parse_date_string;
use crate::{errors::ExecutorError, evaluator::ExpressionEvaluator, schema::CombinedSchema};

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

impl CompareOp {
    /// Build a `CompareOp` from a binary comparison operator, if the operator
    /// is one of the six comparison operators. Returns `None` otherwise.
    fn from_binary_operator(op: &BinaryOperator) -> Option<CompareOp> {
        Some(match op {
            BinaryOperator::LessThan => CompareOp::LessThan,
            BinaryOperator::GreaterThan => CompareOp::GreaterThan,
            BinaryOperator::LessThanOrEqual => CompareOp::LessThanOrEqual,
            BinaryOperator::GreaterThanOrEqual => CompareOp::GreaterThanOrEqual,
            BinaryOperator::Equal => CompareOp::Equal,
            BinaryOperator::NotEqual => CompareOp::NotEqual,
            _ => return None,
        })
    }

    /// Reverse the operand order of the comparison, so that `value op column`
    /// becomes the equivalent `column reversed(op) value`. Equality and
    /// inequality are symmetric and unchanged.
    fn reversed(self) -> CompareOp {
        match self {
            CompareOp::LessThan => CompareOp::GreaterThan,
            CompareOp::GreaterThan => CompareOp::LessThan,
            CompareOp::LessThanOrEqual => CompareOp::GreaterThanOrEqual,
            CompareOp::GreaterThanOrEqual => CompareOp::LessThanOrEqual,
            CompareOp::Equal => CompareOp::Equal,
            CompareOp::NotEqual => CompareOp::NotEqual,
        }
    }
}

/// An arithmetic expression tree over columns and constants, resolved to
/// column *indices* (not names) so it can be evaluated against a columnar
/// batch and remapped alongside other predicates during selective column
/// extraction.
///
/// This is the left-hand side of a [`ColumnPredicate::ComputedCompare`]. It is
/// deliberately restricted to the numeric arithmetic that the row path and the
/// columnar path evaluate identically (see `extract_derived_expr`): column
/// references, numeric literals, unary minus, and `+ - * /` binary operators.
/// Scalar functions, casts, and string/date arithmetic are intentionally
/// excluded in Phase 1 (issue #5994) and fall back to the row path.
#[derive(Debug, Clone)]
pub enum DerivedExpr {
    /// A reference to a batch column by index.
    Column(usize),
    /// A constant literal value.
    Literal(SqlValue),
    /// A binary arithmetic operation (`+`, `-`, `*`, `/`).
    BinaryOp { left: Box<DerivedExpr>, op: BinaryOperator, right: Box<DerivedExpr> },
    /// Unary negation (`-x`).
    Negate(Box<DerivedExpr>),
}

impl DerivedExpr {
    /// Append every column index referenced anywhere in the tree to `out`.
    fn collect_columns(&self, out: &mut Vec<usize>) {
        match self {
            DerivedExpr::Column(idx) => out.push(*idx),
            DerivedExpr::Literal(_) => {}
            DerivedExpr::BinaryOp { left, right, .. } => {
                left.collect_columns(out);
                right.collect_columns(out);
            }
            DerivedExpr::Negate(inner) => inner.collect_columns(out),
        }
    }

    /// Return a copy of the tree with every column index remapped through
    /// `remap` (used when the batch is selectively extracted and columns are
    /// renumbered).
    fn remap<F: Fn(usize) -> usize + Copy>(&self, remap: F) -> DerivedExpr {
        match self {
            DerivedExpr::Column(idx) => DerivedExpr::Column(remap(*idx)),
            DerivedExpr::Literal(v) => DerivedExpr::Literal(v.clone()),
            DerivedExpr::BinaryOp { left, op, right } => DerivedExpr::BinaryOp {
                left: Box::new(left.remap(remap)),
                op: *op,
                right: Box::new(right.remap(remap)),
            },
            DerivedExpr::Negate(inner) => DerivedExpr::Negate(Box::new(inner.remap(remap))),
        }
    }

    /// Evaluate the arithmetic tree for a single row against `get_value`,
    /// which returns the column value at a given index (or `None` when the
    /// value is absent). Uses the row-path arithmetic evaluator
    /// (`eval_binary_op_static`) so that overflow, division-by-zero, and NULL
    /// propagation match the row path exactly (issue #5994 correctness bar).
    ///
    /// Returns `SqlValue::Null` if any referenced column value is NULL or
    /// absent (NULL propagation), matching the row path.
    pub fn evaluate_row<F>(&self, get_value: &mut F) -> Result<SqlValue, ExecutorError>
    where
        F: FnMut(usize) -> Option<SqlValue>,
    {
        match self {
            DerivedExpr::Column(idx) => Ok(get_value(*idx).unwrap_or(SqlValue::Null)),
            DerivedExpr::Literal(v) => Ok(v.clone()),
            DerivedExpr::BinaryOp { left, op, right } => {
                let left_val = left.evaluate_row(get_value)?;
                let right_val = right.evaluate_row(get_value)?;
                ExpressionEvaluator::eval_binary_op_static(
                    &left_val,
                    op,
                    &right_val,
                    SqlMode::default(),
                )
            }
            DerivedExpr::Negate(inner) => {
                let inner_val = inner.evaluate_row(get_value)?;
                if matches!(inner_val, SqlValue::Null) {
                    return Ok(SqlValue::Null);
                }
                ExpressionEvaluator::eval_binary_op_static(
                    &SqlValue::Integer(0),
                    &BinaryOperator::Minus,
                    &inner_val,
                    SqlMode::default(),
                )
            }
        }
    }
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

    /// column IS NULL — the row matches iff the column value is NULL.
    ///
    /// Evaluated directly from the column's null bitmap (the mask is exactly
    /// the bitmap; an absent bitmap yields an all-false mask), so no value
    /// comparison is performed. See `evaluate_null_predicate_range`.
    IsNull { column_idx: usize },

    /// column IS NOT NULL — the row matches iff the column value is non-NULL.
    ///
    /// The complement of `IsNull`: the mask is the negation of the null bitmap
    /// (an absent bitmap yields an all-true mask).
    IsNotNull { column_idx: usize },

    /// `<numeric-arith-over-columns> op value` — a comparison whose left-hand
    /// side is a computed arithmetic expression over one or more columns
    /// (issue #5994). The derived value is materialized per row via the
    /// row-path arithmetic evaluator (`DerivedExpr::evaluate_row`) and compared
    /// against `value`; a NULL derived value (from NULL/absent inputs) is
    /// non-matching, matching the row path.
    ///
    /// `columns` caches the referenced column indices so `referenced_columns`
    /// stays O(1) to serve; it is kept in sync with `expr` on remap.
    ComputedCompare { expr: DerivedExpr, op: CompareOp, value: SqlValue, columns: Vec<usize> },
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
            | ColumnPredicate::InList { column_idx, .. }
            | ColumnPredicate::IsNull { column_idx }
            | ColumnPredicate::IsNotNull { column_idx } => vec![*column_idx],
            ColumnPredicate::ColumnCompare { left_column_idx, right_column_idx, .. } => {
                vec![*left_column_idx, *right_column_idx]
            }
            ColumnPredicate::ComputedCompare { columns, .. } => columns.clone(),
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
        ColumnPredicate::IsNull { column_idx } => {
            ColumnPredicate::IsNull { column_idx: find_new_idx(*column_idx) }
        }
        ColumnPredicate::IsNotNull { column_idx } => {
            ColumnPredicate::IsNotNull { column_idx: find_new_idx(*column_idx) }
        }
        ColumnPredicate::ComputedCompare { expr, op, value, columns } => {
            let new_expr = expr.remap(find_new_idx);
            let mut new_columns: Vec<usize> = columns.iter().map(|c| find_new_idx(*c)).collect();
            new_columns.sort_unstable();
            new_columns.dedup();
            ColumnPredicate::ComputedCompare {
                expr: new_expr,
                op: *op,
                value: value.clone(),
                columns: new_columns,
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
            // Issue #5792: comparisons on a non-BINARY-collated column (e.g.
            // NOCASE) must apply the collation; the columnar comparators
            // compare raw values, so decline pushdown and fall back to the
            // collation-aware expression evaluator.
            !schema.column_has_non_binary_collation(*column_idx)
                && value_supported_for_column(schema.get_column_type_by_index(*column_idx), value)
        }
        ColumnPredicate::Between { column_idx, low, high } => {
            let col_type = schema.get_column_type_by_index(*column_idx);
            // Issue #5792: see above — collated columns need the full evaluator.
            !schema.column_has_non_binary_collation(*column_idx)
                && value_supported_for_column(col_type, low)
                && value_supported_for_column(col_type, high)
        }
        ColumnPredicate::InList { column_idx, values, .. } => {
            let col_type = schema.get_column_type_by_index(*column_idx);
            // Issue #5806: IN on a non-BINARY-collated column (e.g. NOCASE)
            // must apply the LHS collation; the columnar comparators compare
            // raw values, so decline pushdown and fall back to the
            // collation-aware expression evaluator.
            !schema.column_has_non_binary_collation(*column_idx)
                && values.iter().all(|v| value_supported_for_column(col_type, v))
        }
        // LIKE patterns are strings; existing comparator behavior applies
        ColumnPredicate::Like { .. } => true,
        // IS NULL / IS NOT NULL read only the column's null bitmap, so they are
        // faithful for every column type and unaffected by collation or type
        // affinity — always supported by the columnar path.
        ColumnPredicate::IsNull { .. } | ColumnPredicate::IsNotNull { .. } => true,
        ColumnPredicate::ColumnCompare { left_column_idx, right_column_idx, .. } => {
            // Issue #5792: see above — collated columns need the full evaluator.
            !schema.column_has_non_binary_collation(*left_column_idx)
                && !schema.column_has_non_binary_collation(*right_column_idx)
                && column_compare_supported(
                    schema.get_column_type_by_index(*left_column_idx),
                    schema.get_column_type_by_index(*right_column_idx),
                )
        }
        // Issue #5994: a computed-column comparison is only ever produced by
        // `extract_derived_expr`, which already restricts the operand columns
        // to numeric, non-collated types and the operators to numeric
        // arithmetic. The derived value is compared numerically via the
        // row-path evaluator, so no collation applies. Re-verify the guard
        // defensively: every referenced column must be numeric and BINARY.
        ColumnPredicate::ComputedCompare { columns, .. } => columns.iter().all(|&col_idx| {
            !schema.column_has_non_binary_collation(col_idx)
                && column_type_is_numeric(schema.get_column_type_by_index(col_idx))
        }),
    }
}

/// Whether a column's declared type is one of the numeric types over which
/// [`DerivedExpr`] arithmetic is faithful to the row path (issue #5994).
/// Unknown types (e.g. outer-scope references) are declined conservatively.
fn column_type_is_numeric(t: Option<&DataType>) -> bool {
    matches!(
        t,
        Some(
            DataType::Integer
                | DataType::Smallint
                | DataType::Bigint
                | DataType::Unsigned
                | DataType::Real
                | DataType::DoublePrecision
                | DataType::Float { .. }
                | DataType::Numeric { .. }
                | DataType::Decimal { .. }
        )
    )
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
        // Issues #5340/#5803: the expression evaluator normalizes Boolean to
        // Integer 0/1 and orders Boolean-vs-string with strict storage-class
        // ordering (numeric < TEXT, no string parsing). The columnar
        // numeric-vs-string arm instead coerces parseable strings to numbers,
        // which would diverge (e.g. `flag = '1'`), so decline string/BLOB/
        // temporal operands and let the evaluator handle them. Boolean vs
        // Boolean and Boolean vs numeric compare faithfully in both paths
        // (booleans coerce to 0/1).
        Some(DataType::Boolean) => matches!(value, SqlValue::Boolean(_)) || is_numeric_value(value),
        // Issue #5765: TEXT-affinity column compared against a numeric literal.
        // SQLite applies TEXT affinity to the literal (renders the number as
        // text) and compares as strings, so `'2' = 2` is true but `'2.0' = 2`
        // is false. The columnar comparators instead coerce the stored text to
        // a number and compare numerically (`2.0 == 2` -> true), wrongly
        // matching both rows. There is no faithful columnar arm, so decline
        // pushdown and let the evaluator's `apply_affinity_for_comparison`
        // Case 1 handle it. NUMERIC/REAL/INTEGER columns keep numeric
        // comparison and are unaffected.
        Some(other) if other.sqlite_affinity() == TypeAffinity::Text && is_numeric_value(value) => {
            false
        }
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
            // Issues #5340/#5803: Boolean literal vs string/BLOB column —
            // the evaluator normalizes Boolean to Integer 0/1 and applies
            // strict storage-class ordering (no string parsing), while the
            // columnar numeric-vs-string arm coerces parseable strings to
            // numbers; decline so the evaluator's semantics win.
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

/// The numeric arithmetic operators over which [`DerivedExpr`] evaluation is
/// faithful to the row path (issue #5994): `+`, `-`, `*`, `/`.
fn is_supported_derived_operator(op: &BinaryOperator) -> bool {
    matches!(
        op,
        BinaryOperator::Plus
            | BinaryOperator::Minus
            | BinaryOperator::Multiply
            | BinaryOperator::Divide
    )
}

/// Try to convert an expression into a [`DerivedExpr`] arithmetic tree over
/// numeric columns and constants (issue #5994).
///
/// Returns `Some((expr, columns))` only when:
/// - every leaf is either a numeric column reference (a bare `ColumnRef` in this scan's schema
///   whose declared type is numeric — see [`column_type_is_numeric`]) or a numeric constant (folded
///   via [`try_fold_constant`]);
/// - every operator is one of `+ - * /` (or unary minus);
/// - the tree references at least one column (a fully-constant tree is not a computed-column
///   predicate — it would fold to a literal and take the plain column-vs-constant path).
///
/// Any other shape — scalar functions, casts, string/date arithmetic, columns
/// from another table, non-numeric columns — returns `None`, so the caller
/// declines pushdown and the WHERE clause falls back to the row path. This
/// narrow guard is what keeps overflow/division/NULL semantics matching the
/// row path: only arithmetic the row-path evaluator handles identically is
/// admitted.
///
/// This is shared with the columnar GROUP BY expression-key path (issue #5995):
/// a `GROUP BY <expr>` that this function admits can be materialized as a
/// derived key column via [`super::super::aggregate::materialize_derived_column`]
/// and fed into the existing hash-grouping machinery, keeping grouping-key
/// arithmetic in lockstep with computed WHERE predicates.
pub fn extract_derived_expr(
    expr: &Expression,
    schema: &CombinedSchema,
) -> Option<(DerivedExpr, Vec<usize>)> {
    fn build(expr: &Expression, schema: &CombinedSchema) -> Option<DerivedExpr> {
        match expr {
            Expression::ColumnRef(col_id) => {
                // Only bare, single-table column references with a numeric type.
                if col_id.schema_canonical().is_some() {
                    return None;
                }
                let table = col_id.table_canonical();
                let column = col_id.column_canonical();
                let column_idx = schema.get_column_index(table, column)?;
                // Collated columns compare with collation semantics the numeric
                // arithmetic path does not model; decline them.
                if schema.column_has_non_binary_collation(column_idx) {
                    return None;
                }
                if !column_type_is_numeric(schema.get_column_type_by_index(column_idx)) {
                    return None;
                }
                Some(DerivedExpr::Column(column_idx))
            }
            Expression::Literal(_) | Expression::BinaryOp { .. } => {
                // A subtree that folds to a numeric constant becomes a Literal.
                if let Some(folded) = try_fold_constant(expr) {
                    if is_numeric_value(&folded) {
                        return Some(DerivedExpr::Literal(folded));
                    }
                    // A constant that isn't numeric (e.g. a string) can't take
                    // the numeric arithmetic path.
                    return None;
                }
                // Non-constant BinaryOp: must be a supported arithmetic op over
                // supported operands.
                if let Expression::BinaryOp { left, op, right } = expr {
                    if !is_supported_derived_operator(op) {
                        return None;
                    }
                    let left_d = build(left, schema)?;
                    let right_d = build(right, schema)?;
                    return Some(DerivedExpr::BinaryOp {
                        left: Box::new(left_d),
                        op: *op,
                        right: Box::new(right_d),
                    });
                }
                None
            }
            Expression::UnaryOp { op: UnaryOperator::Minus, expr: inner } => {
                Some(DerivedExpr::Negate(Box::new(build(inner, schema)?)))
            }
            Expression::UnaryOp { op: UnaryOperator::Plus, expr: inner } => build(inner, schema),
            _ => None,
        }
    }

    let derived = build(expr, schema)?;
    let mut columns = Vec::new();
    derived.collect_columns(&mut columns);
    columns.sort_unstable();
    columns.dedup();
    // Require at least one column: a fully-constant tree is not a
    // computed-column predicate.
    if columns.is_empty() {
        return None;
    }
    Some((derived, columns))
}

/// Try to build a [`ColumnPredicate::ComputedCompare`] leaf from a binary
/// comparison whose left or right operand is a computed numeric arithmetic
/// tree over columns and the other operand folds to a numeric constant
/// (issue #5994).
///
/// Handles both `<arith> op const` and `const op <arith>` (reversing the
/// operator for the latter). Returns `None` if neither side is a supported
/// derived expression, if the other side is not a numeric constant, or if `op`
/// is not a comparison operator.
fn try_computed_compare(
    left: &Expression,
    op: &BinaryOperator,
    right: &Expression,
    schema: &CombinedSchema,
) -> Option<ColumnPredicate> {
    let compare_op = CompareOp::from_binary_operator(op)?;

    // Try: <derived-arith> op const
    if let Some((derived, columns)) = extract_derived_expr(left, schema) {
        if let Some(value) = try_fold_constant(right) {
            if is_numeric_value(&value) {
                return Some(ColumnPredicate::ComputedCompare {
                    expr: derived,
                    op: compare_op,
                    value,
                    columns,
                });
            }
        }
        return None;
    }

    // Try: const op <derived-arith>  (reverse the comparison operator)
    if let Some((derived, columns)) = extract_derived_expr(right, schema) {
        if let Some(value) = try_fold_constant(left) {
            if is_numeric_value(&value) {
                return Some(ColumnPredicate::ComputedCompare {
                    expr: derived,
                    op: compare_op.reversed(),
                    value,
                    columns,
                });
            }
        }
    }

    None
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

            // Try: <numeric-arith-over-cols> op const  (or const op <arith>)
            // Issue #5994: computed-column comparison on the columnar path.
            if let Some(predicate) = try_computed_compare(left, op, right, schema) {
                return Some(PredicateTree::Leaf(predicate));
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

        // IS NULL / IS NOT NULL on a bare column: read the null bitmap directly.
        // `negated` distinguishes the two (false = IS NULL, true = IS NOT NULL),
        // which drives the choice of predicate variant (three-valued logic).
        // A non-column operand has no null bitmap to consume, so decline it.
        Expression::IsNull { expr: inner, negated } => {
            if let Expression::ColumnRef(col_id) = inner.as_ref() {
                let table = col_id.table_canonical();
                let column = col_id.column_canonical();
                let column_idx = schema.get_column_index(table, column)?;
                let predicate = if *negated {
                    ColumnPredicate::IsNotNull { column_idx }
                } else {
                    ColumnPredicate::IsNull { column_idx }
                };
                return Some(PredicateTree::Leaf(predicate));
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

            // Try: <numeric-arith-over-cols> op const  (or const op <arith>)
            // Issue #5994: computed-column comparison on the columnar path.
            if let Some(predicate) = try_computed_compare(left, op, right, schema) {
                predicates.push(predicate);
                return Some(());
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

        // IS NULL / IS NOT NULL on a bare column: read the null bitmap directly.
        // Skip (don't fail) if the column is not in this scan's schema, matching
        // the cross-table-predicate handling of the other arms.
        Expression::IsNull { expr: inner, negated } => {
            if let Expression::ColumnRef(col_id) = inner.as_ref() {
                let table = col_id.table_canonical();
                let column = col_id.column_canonical();
                if let Some(column_idx) = schema.get_column_index(table, column) {
                    predicates.push(if *negated {
                        ColumnPredicate::IsNotNull { column_idx }
                    } else {
                        ColumnPredicate::IsNull { column_idx }
                    });
                }
            }
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

    fn is_null_expr(column: &str, negated: bool) -> Expression {
        Expression::IsNull {
            expr: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                column, false,
            ))),
            negated,
        }
    }

    #[test]
    fn test_extract_is_null_tree() {
        let schema = create_test_schema();

        // col0 IS NULL -> IsNull { column_idx: 0 }
        let tree = extract_predicate_tree(&is_null_expr("col0", false), &schema, false)
            .expect("IS NULL should be columnar-convertible");
        match tree {
            PredicateTree::Leaf(ColumnPredicate::IsNull { column_idx }) => {
                assert_eq!(column_idx, 0)
            }
            other => panic!("expected IsNull leaf, got {other:?}"),
        }

        // col1 IS NOT NULL -> IsNotNull { column_idx: 1 } (negated flag drives choice)
        let tree = extract_predicate_tree(&is_null_expr("col1", true), &schema, false)
            .expect("IS NOT NULL should be columnar-convertible");
        match tree {
            PredicateTree::Leaf(ColumnPredicate::IsNotNull { column_idx }) => {
                assert_eq!(column_idx, 1)
            }
            other => panic!("expected IsNotNull leaf, got {other:?}"),
        }
    }

    #[test]
    fn test_extract_is_null_non_column_declined() {
        let schema = create_test_schema();

        // (1 + 2) IS NULL — non-column operand has no null bitmap to consume.
        let expr = Expression::IsNull {
            expr: Box::new(Expression::Literal(SqlValue::Integer(1))),
            negated: false,
        };
        assert!(extract_predicate_tree(&expr, &schema, false).is_none());
    }

    #[test]
    fn test_extract_is_null_compound_and() {
        let schema = create_test_schema();

        // col0 IS NULL AND col1 > 5
        let expr = Expression::BinaryOp {
            left: Box::new(is_null_expr("col0", false)),
            op: BinaryOperator::And,
            right: Box::new(comparison_expr(
                "col1",
                BinaryOperator::GreaterThan,
                SqlValue::Integer(5),
            )),
        };
        let tree = extract_predicate_tree(&expr, &schema, false)
            .expect("compound IS NULL AND compare should be columnar-convertible");
        match tree {
            PredicateTree::And(children) => {
                assert_eq!(children.len(), 2);
                assert!(matches!(
                    children[0],
                    PredicateTree::Leaf(ColumnPredicate::IsNull { column_idx: 0 })
                ));
                assert!(matches!(
                    children[1],
                    PredicateTree::Leaf(ColumnPredicate::GreaterThan { column_idx: 1, .. })
                ));
            }
            other => panic!("expected And node, got {other:?}"),
        }
    }

    #[test]
    fn test_extract_is_null_legacy_and_extractor() {
        let schema = create_test_schema();

        // Legacy AND-only extractor: col0 IS NULL AND col1 IS NOT NULL
        let expr = Expression::BinaryOp {
            left: Box::new(is_null_expr("col0", false)),
            op: BinaryOperator::And,
            right: Box::new(is_null_expr("col1", true)),
        };
        let predicates = extract_column_predicates(&expr, &schema, false)
            .expect("legacy extractor should return both null-test predicates");
        assert_eq!(predicates.len(), 2);
        assert!(matches!(predicates[0], ColumnPredicate::IsNull { column_idx: 0 }));
        assert!(matches!(predicates[1], ColumnPredicate::IsNotNull { column_idx: 1 }));
    }

    #[test]
    fn test_is_null_referenced_columns_and_remap() {
        let is_null = ColumnPredicate::IsNull { column_idx: 7 };
        let is_not_null = ColumnPredicate::IsNotNull { column_idx: 3 };
        assert_eq!(is_null.referenced_columns(), vec![7]);
        assert_eq!(is_not_null.referenced_columns(), vec![3]);

        // Remap against a sparse column mapping [3, 7] -> new indices 0, 1.
        let mapping = vec![3usize, 7usize];
        let remapped = remap_predicates(&[is_null, is_not_null], &mapping);
        assert!(matches!(remapped[0], ColumnPredicate::IsNull { column_idx: 1 }));
        assert!(matches!(remapped[1], ColumnPredicate::IsNotNull { column_idx: 0 }));
    }

    // ---- Issue #5994: computed-column comparison extraction ----

    fn col(name: &str) -> Expression {
        Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(name, false))
    }

    fn binop(left: Expression, op: BinaryOperator, right: Expression) -> Expression {
        Expression::BinaryOp { left: Box::new(left), op, right: Box::new(right) }
    }

    /// `col0 * col1 > 100` extracts a ComputedCompare referencing both columns.
    #[test]
    fn test_extract_computed_compare_mul_over_columns() {
        let schema = create_test_schema();
        // col0 * col1 > 100
        let expr = binop(
            binop(col("col0"), BinaryOperator::Multiply, col("col1")),
            BinaryOperator::GreaterThan,
            Expression::Literal(SqlValue::Integer(100)),
        );

        let tree = extract_predicate_tree(&expr, &schema, false)
            .expect("computed-column comparison should extract on the tree path");
        match tree {
            PredicateTree::Leaf(ColumnPredicate::ComputedCompare {
                op,
                ref value,
                ref columns,
                ..
            }) => {
                assert_eq!(op, CompareOp::GreaterThan);
                assert_eq!(*value, SqlValue::Integer(100));
                assert_eq!(*columns, vec![0, 1]);
            }
            other => panic!("expected ComputedCompare leaf, got {other:?}"),
        }

        // Legacy extractor produces the same predicate.
        let preds = extract_column_predicates(&expr, &schema, false)
            .expect("legacy extractor should also produce ComputedCompare");
        assert_eq!(preds.len(), 1);
        assert!(matches!(preds[0], ColumnPredicate::ComputedCompare { .. }));
        assert_eq!(preds[0].referenced_columns(), vec![0, 1]);
    }

    /// `const op <arith>` reverses the comparison operator.
    #[test]
    fn test_extract_computed_compare_reversed() {
        let schema = create_test_schema();
        // 100 < col0 - col1   ==>   (col0 - col1) > 100
        let expr = binop(
            Expression::Literal(SqlValue::Integer(100)),
            BinaryOperator::LessThan,
            binop(col("col0"), BinaryOperator::Minus, col("col1")),
        );
        let tree = extract_predicate_tree(&expr, &schema, false).expect("should extract");
        match tree {
            PredicateTree::Leaf(ColumnPredicate::ComputedCompare { op, ref value, .. }) => {
                assert_eq!(op, CompareOp::GreaterThan);
                assert_eq!(*value, SqlValue::Integer(100));
            }
            other => panic!("expected ComputedCompare leaf, got {other:?}"),
        }
    }

    /// Remapping a ComputedCompare renumbers both the cached column list and
    /// the embedded DerivedExpr column indices.
    #[test]
    fn test_computed_compare_remap() {
        let schema = create_test_schema();
        let expr = binop(
            binop(col("col0"), BinaryOperator::Plus, col("col1")),
            BinaryOperator::LessThanOrEqual,
            Expression::Literal(SqlValue::Integer(5)),
        );
        let preds = extract_column_predicates(&expr, &schema, false).unwrap();
        let mapping = vec![0usize, 1usize];
        let remapped = remap_predicates(&preds, &mapping);
        match &remapped[0] {
            ColumnPredicate::ComputedCompare { columns, expr, .. } => {
                assert_eq!(*columns, vec![0, 1]);
                let mut cols = Vec::new();
                expr.collect_columns(&mut cols);
                cols.sort_unstable();
                cols.dedup();
                assert_eq!(cols, vec![0, 1]);
            }
            other => panic!("expected ComputedCompare, got {other:?}"),
        }
    }

    /// Unsupported operators (e.g. modulo) and non-numeric operands decline
    /// extraction so the WHERE clause falls back to the row path.
    #[test]
    fn test_computed_compare_declines_unsupported() {
        let schema = create_test_schema();

        // Modulo is not one of + - * / — decline (falls through to None).
        let expr = binop(
            binop(col("col0"), BinaryOperator::Modulo, col("col1")),
            BinaryOperator::Equal,
            Expression::Literal(SqlValue::Integer(0)),
        );
        assert!(extract_predicate_tree(&expr, &schema, false).is_none());

        // Non-numeric constant on the compared side declines.
        let expr = binop(
            binop(col("col0"), BinaryOperator::Plus, col("col1")),
            BinaryOperator::Equal,
            Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("x"))),
        );
        assert!(extract_predicate_tree(&expr, &schema, false).is_none());
    }

    /// A string column in the arithmetic tree declines (numeric-only Phase 1).
    #[test]
    fn test_computed_compare_declines_non_numeric_column() {
        let schema = TableSchema::new(
            "t".to_string(),
            vec![
                ColumnSchema::new("n".to_string(), DataType::Integer, false),
                ColumnSchema::new("s".to_string(), DataType::Varchar { max_length: None }, true),
            ],
        );
        let schema = CombinedSchema::from_table("t".to_string(), schema);
        // n * s > 1  — s is a string column, decline.
        let expr = binop(
            binop(col("n"), BinaryOperator::Multiply, col("s")),
            BinaryOperator::GreaterThan,
            Expression::Literal(SqlValue::Integer(1)),
        );
        assert!(extract_predicate_tree(&expr, &schema, false).is_none());
    }

    /// The DerivedExpr evaluator matches the row-path arithmetic semantics,
    /// including NULL propagation and i64-overflow → Double fallback.
    #[test]
    fn test_derived_expr_evaluate_row_parity() {
        // (col0 * col1) with col0=6, col1=7 => 42 (Integer)
        let expr = DerivedExpr::BinaryOp {
            left: Box::new(DerivedExpr::Column(0)),
            op: BinaryOperator::Multiply,
            right: Box::new(DerivedExpr::Column(1)),
        };
        let mut vals = |idx: usize| match idx {
            0 => Some(SqlValue::Integer(6)),
            1 => Some(SqlValue::Integer(7)),
            _ => None,
        };
        assert_eq!(expr.evaluate_row(&mut vals).unwrap(), SqlValue::Integer(42));

        // NULL propagation: any NULL input => NULL derived value.
        let mut vals_null = |idx: usize| match idx {
            0 => Some(SqlValue::Integer(6)),
            1 => Some(SqlValue::Null),
            _ => None,
        };
        assert_eq!(expr.evaluate_row(&mut vals_null).unwrap(), SqlValue::Null);

        // i64 overflow: i64::MAX * 2 must fall back to Double (row-path parity),
        // never wrap or panic.
        let mut vals_ovf = |idx: usize| match idx {
            0 => Some(SqlValue::Integer(i64::MAX)),
            1 => Some(SqlValue::Integer(2)),
            _ => None,
        };
        let result = expr.evaluate_row(&mut vals_ovf).unwrap();
        assert!(
            matches!(result, SqlValue::Double(_) | SqlValue::Float(_) | SqlValue::Numeric(_)),
            "overflow must fall back to float, got {result:?}"
        );
    }
}
