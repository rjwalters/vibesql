//! Compiled predicates for fast evaluation
//!
//! This module provides pre-compiled predicates that bypass the full expression
//! evaluation machinery for simple predicates like `col = literal`. This avoids:
//! - CSE cache creation and clearing
//! - Expression tree traversal
//! - `is_deterministic()` checks
//! - Depth tracking overhead
//!
//! # Performance
//! For simple predicates in OLTP workloads (e.g., TPC-C), this can provide
//! 10-50x improvement in predicate evaluation throughput.

use vibesql_ast::{BinaryOperator, Expression};
use vibesql_types::SqlValue;

use super::expressions::eval::format_float_for_text_comparison;
use crate::schema::CombinedSchema;

/// A compiled predicate that can be evaluated efficiently without expression traversal
#[derive(Debug)]
pub enum CompiledPredicate {
    /// Simple equality: column = literal
    /// Stores (column_index, literal_value)
    Equals { col_idx: usize, value: SqlValue },

    /// Simple inequality: column != literal
    NotEquals { col_idx: usize, value: SqlValue },

    /// Range comparison: column <op> literal
    /// where op is <, <=, >, >=
    Range { col_idx: usize, op: RangeOp, value: SqlValue },

    /// IS NULL check
    IsNull { col_idx: usize },

    /// IS NOT NULL check
    IsNotNull { col_idx: usize },

    /// AND of two compiled predicates (legacy binary form)
    And(Box<CompiledPredicate>, Box<CompiledPredicate>),

    /// OR of two compiled predicates (legacy binary form)
    Or(Box<CompiledPredicate>, Box<CompiledPredicate>),

    /// N-ary AND of multiple compiled predicates (flat conjunction)
    Conjunction(Vec<CompiledPredicate>),

    /// N-ary OR of multiple compiled predicates (flat disjunction)
    Disjunction(Vec<CompiledPredicate>),

    /// Fallback: complex predicate that needs full evaluation
    /// This is used when we can't compile the predicate.
    /// The Expression is stored for potential future use but not used by evaluate().
    #[allow(dead_code)]
    Complex(Expression),
}

/// Range comparison operator
#[derive(Debug, Clone, Copy)]
pub enum RangeOp {
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
}

impl CompiledPredicate {
    /// Try to compile an expression into a fast-path predicate
    ///
    /// Returns `Some(compiled)` if the expression can be compiled,
    /// or wraps it in `Complex` if it needs full evaluation.
    pub fn compile(expr: &Expression, schema: &CombinedSchema) -> Self {
        Self::try_compile(expr, schema).unwrap_or_else(|| CompiledPredicate::Complex(expr.clone()))
    }

    /// Try to compile an expression, returning None if not possible
    fn try_compile(expr: &Expression, schema: &CombinedSchema) -> Option<Self> {
        match expr {
            // Simple binary operations: col = literal, col > literal, etc.
            Expression::BinaryOp { left, op, right } => {
                Self::try_compile_binary_op(left, op, right, schema)
            }

            // Flattened conjunction (AND chain)
            Expression::Conjunction(children) => {
                let compiled: Vec<_> =
                    children.iter().filter_map(|child| Self::try_compile(child, schema)).collect();

                // All children must be compilable
                if compiled.len() != children.len() {
                    return None;
                }

                // Check none are Complex
                if compiled.iter().any(|c| matches!(c, CompiledPredicate::Complex(_))) {
                    return None;
                }

                Some(CompiledPredicate::Conjunction(compiled))
            }

            // Flattened disjunction (OR chain)
            Expression::Disjunction(children) => {
                let compiled: Vec<_> =
                    children.iter().filter_map(|child| Self::try_compile(child, schema)).collect();

                // All children must be compilable
                if compiled.len() != children.len() {
                    return None;
                }

                // Check none are Complex
                if compiled.iter().any(|c| matches!(c, CompiledPredicate::Complex(_))) {
                    return None;
                }

                Some(CompiledPredicate::Disjunction(compiled))
            }

            // IS NULL / IS NOT NULL
            Expression::IsNull { expr, negated } => {
                if let Expression::ColumnRef(col_id) = expr.as_ref() {
                    let col_idx = schema
                        .get_column_index(col_id.table_canonical(), col_id.column_canonical())?;
                    if *negated {
                        Some(CompiledPredicate::IsNotNull { col_idx })
                    } else {
                        Some(CompiledPredicate::IsNull { col_idx })
                    }
                } else {
                    None
                }
            }

            // Literals that are always true/false (constant folding already done)
            Expression::Literal(SqlValue::Boolean(true)) => {
                // Always true - we can represent this as a tautology
                // but for now, fall back to complex
                None
            }
            Expression::Literal(SqlValue::Boolean(false)) => {
                // Always false - we can represent this as a contradiction
                // but for now, fall back to complex
                None
            }

            _ => None,
        }
    }

    /// Try to compile a binary operation
    fn try_compile_binary_op(
        left: &Expression,
        op: &BinaryOperator,
        right: &Expression,
        schema: &CombinedSchema,
    ) -> Option<Self> {
        // Handle AND/OR by recursively compiling sub-predicates
        match op {
            BinaryOperator::And => {
                let left_compiled = Self::try_compile(left, schema)?;
                let right_compiled = Self::try_compile(right, schema)?;

                // Check if both sides are compilable (not Complex)
                if matches!(left_compiled, CompiledPredicate::Complex(_))
                    || matches!(right_compiled, CompiledPredicate::Complex(_))
                {
                    return None;
                }

                Some(CompiledPredicate::And(Box::new(left_compiled), Box::new(right_compiled)))
            }

            BinaryOperator::Or => {
                let left_compiled = Self::try_compile(left, schema)?;
                let right_compiled = Self::try_compile(right, schema)?;

                // Check if both sides are compilable (not Complex)
                if matches!(left_compiled, CompiledPredicate::Complex(_))
                    || matches!(right_compiled, CompiledPredicate::Complex(_))
                {
                    return None;
                }

                Some(CompiledPredicate::Or(Box::new(left_compiled), Box::new(right_compiled)))
            }

            // Simple comparison: col <op> literal or literal <op> col
            _ => Self::try_compile_comparison(left, op, right, schema),
        }
    }

    /// Try to compile a simple comparison (col <op> literal)
    fn try_compile_comparison(
        left: &Expression,
        op: &BinaryOperator,
        right: &Expression,
        schema: &CombinedSchema,
    ) -> Option<Self> {
        // Try col <op> literal
        if let (Expression::ColumnRef(col_id), Expression::Literal(value)) = (left, right) {
            let col_idx =
                schema.get_column_index(col_id.table_canonical(), col_id.column_canonical())?;
            // Issue #5792: comparisons against a non-BINARY-collated column
            // (e.g. NOCASE) must apply the collation; the compiled fast path
            // compares raw values, so decline and fall back to the
            // collation-aware expression evaluator.
            if schema.column_has_non_binary_collation(col_idx) {
                return None;
            }
            if !Self::literal_supported_for_column(schema, col_idx, value) {
                return None;
            }
            let value = Self::coerce_literal_for_column(schema, col_idx, value.clone());
            return Self::compile_comparison_with_idx(col_idx, op, value, false);
        }

        // Try literal <op> col (reverse the operator)
        if let (Expression::Literal(value), Expression::ColumnRef(col_id)) = (left, right) {
            let col_idx =
                schema.get_column_index(col_id.table_canonical(), col_id.column_canonical())?;
            // Issue #5792: see above — collated columns need the full evaluator.
            if schema.column_has_non_binary_collation(col_idx) {
                return None;
            }
            if !Self::literal_supported_for_column(schema, col_idx, value) {
                return None;
            }
            let value = Self::coerce_literal_for_column(schema, col_idx, value.clone());
            return Self::compile_comparison_with_idx(col_idx, op, value, true);
        }

        None
    }

    /// Issue #5765: apply the column's TEXT affinity to a numeric literal at
    /// compile time so the compiled fast path matches the interpreted
    /// evaluator's `apply_affinity_for_comparison` Case 1
    /// (`evaluator/expressions/eval.rs`).
    ///
    /// SQLite rule: comparing a TEXT-affinity column against a numeric literal
    /// applies TEXT affinity to the literal (renders the number as text) and
    /// performs a string comparison — so `'2' = 2` is true but `'2.0' = 2` is
    /// false (`'2.0' != '2'`). Without this, `values_equal` would parse the
    /// stored text as a number and compare numerically (`2.0 == 2` -> true),
    /// wrongly matching both rows.
    ///
    /// The numeric->text rendering mirrors Case 1 exactly: integers via
    /// `to_string()`, floating-point via `format_float_for_text_comparison`
    /// (which preserves the decimal point, so TEXT `'10'` does NOT equal REAL
    /// `10.0`). Returns the literal unchanged for non-TEXT columns (NUMERIC /
    /// REAL / INTEGER affinity keep numeric comparison) and for non-numeric
    /// literals.
    fn coerce_literal_for_column(
        schema: &CombinedSchema,
        col_idx: usize,
        value: SqlValue,
    ) -> SqlValue {
        use vibesql_types::TypeAffinity;

        // Only TEXT-affinity columns coerce the literal. Bare columns (no
        // declared type) have NONE affinity and use type ordering instead, so
        // leave them alone — matching the evaluator, which restricts Case 1 to
        // `TypeAffinity::Text`.
        match schema.get_column_type_by_index(col_idx) {
            Some(col_type) if col_type.sqlite_affinity() == TypeAffinity::Text => {}
            _ => return value,
        }

        match value {
            SqlValue::Integer(n) => SqlValue::Varchar(arcstr::ArcStr::from(n.to_string())),
            SqlValue::Smallint(n) => SqlValue::Varchar(arcstr::ArcStr::from(n.to_string())),
            SqlValue::Bigint(n) => SqlValue::Varchar(arcstr::ArcStr::from(n.to_string())),
            SqlValue::Unsigned(n) => SqlValue::Varchar(arcstr::ArcStr::from(n.to_string())),
            SqlValue::Float(n) => SqlValue::Varchar(arcstr::ArcStr::from(
                format_float_for_text_comparison(f64::from(n)),
            )),
            SqlValue::Real(n) => {
                SqlValue::Varchar(arcstr::ArcStr::from(format_float_for_text_comparison(n)))
            }
            SqlValue::Double(n) => {
                SqlValue::Varchar(arcstr::ArcStr::from(format_float_for_text_comparison(n)))
            }
            SqlValue::Numeric(n) => {
                SqlValue::Varchar(arcstr::ArcStr::from(format_float_for_text_comparison(n)))
            }
            // Non-numeric literals (already text, temporal, blob, etc.) are
            // unaffected by TEXT affinity coercion.
            other => other,
        }
    }

    /// Issue #5335: decide whether `values_equal` / `compare_range` can
    /// evaluate this column/literal pairing with the same semantics as the
    /// full expression evaluator. Returns false to decline compilation (the
    /// predicate becomes `Complex` and the full evaluator runs instead).
    fn literal_supported_for_column(
        schema: &CombinedSchema,
        col_idx: usize,
        value: &SqlValue,
    ) -> bool {
        use std::str::FromStr;

        use vibesql_types::DataType;

        let col_type = schema.get_column_type_by_index(col_idx);
        let is_string_col = |t: &DataType| {
            matches!(
                t,
                DataType::Character { .. }
                    | DataType::Varchar { .. }
                    | DataType::CharacterLargeObject
                    | DataType::Name
            )
        };

        match value {
            // Strings against a DATE column compare parse-first; an
            // unparseable string must raise the evaluator's type-mismatch
            // error, which the compiled fast path cannot do (its evaluate()
            // has no error channel and callers treat None as exclude).
            // Strings against TIMESTAMP/TIME columns compare TEXT renderings
            // (#5329) - but numeric-parseable strings are coerced to numbers
            // by the evaluator's NUMERIC-affinity rules first (temporal vs
            // numeric is then always false), so decline those for parity.
            SqlValue::Varchar(s) | SqlValue::Character(s) => match col_type {
                Some(DataType::Date) => vibesql_types::Date::from_str(s).is_ok(),
                Some(DataType::Timestamp { .. }) | Some(DataType::Time { .. }) => {
                    let trimmed = s.trim();
                    trimmed.parse::<i64>().is_err() && trimmed.parse::<f64>().is_err()
                }
                _ => true,
            },
            // Temporal literals are supported against the matching temporal
            // column type and against string columns (TEXT-rendering /
            // parse-first arms). Other known column types have no compiled
            // arm with evaluator-equivalent semantics.
            SqlValue::Timestamp(_) => match col_type {
                Some(DataType::Timestamp { .. }) | None => true,
                Some(t) => is_string_col(t),
            },
            SqlValue::Time(_) => match col_type {
                Some(DataType::Time { .. }) | None => true,
                Some(t) => is_string_col(t),
            },
            SqlValue::Date(_) => match col_type {
                Some(DataType::Date) | None => true,
                Some(t) => is_string_col(t),
            },
            _ => true,
        }
    }

    /// Compile a comparison with a known column index
    /// `reversed` is true when the literal was on the left side
    fn compile_comparison_with_idx(
        col_idx: usize,
        op: &BinaryOperator,
        value: SqlValue,
        reversed: bool,
    ) -> Option<Self> {
        match op {
            BinaryOperator::Equal => Some(CompiledPredicate::Equals { col_idx, value }),
            BinaryOperator::NotEqual => Some(CompiledPredicate::NotEquals { col_idx, value }),

            BinaryOperator::LessThan => {
                if reversed {
                    // literal < col => col > literal
                    Some(CompiledPredicate::Range { col_idx, op: RangeOp::GreaterThan, value })
                } else {
                    Some(CompiledPredicate::Range { col_idx, op: RangeOp::LessThan, value })
                }
            }

            BinaryOperator::LessThanOrEqual => {
                if reversed {
                    // literal <= col => col >= literal
                    Some(CompiledPredicate::Range {
                        col_idx,
                        op: RangeOp::GreaterThanOrEqual,
                        value,
                    })
                } else {
                    Some(CompiledPredicate::Range { col_idx, op: RangeOp::LessThanOrEqual, value })
                }
            }

            BinaryOperator::GreaterThan => {
                if reversed {
                    // literal > col => col < literal
                    Some(CompiledPredicate::Range { col_idx, op: RangeOp::LessThan, value })
                } else {
                    Some(CompiledPredicate::Range { col_idx, op: RangeOp::GreaterThan, value })
                }
            }

            BinaryOperator::GreaterThanOrEqual => {
                if reversed {
                    // literal >= col => col <= literal
                    Some(CompiledPredicate::Range { col_idx, op: RangeOp::LessThanOrEqual, value })
                } else {
                    Some(CompiledPredicate::Range {
                        col_idx,
                        op: RangeOp::GreaterThanOrEqual,
                        value,
                    })
                }
            }

            _ => None,
        }
    }

    /// Check if this predicate is fully compiled (no Complex fallback)
    #[inline]
    pub fn is_fully_compiled(&self) -> bool {
        match self {
            CompiledPredicate::Complex(_) => false,
            CompiledPredicate::And(left, right) | CompiledPredicate::Or(left, right) => {
                left.is_fully_compiled() && right.is_fully_compiled()
            }
            CompiledPredicate::Conjunction(children) | CompiledPredicate::Disjunction(children) => {
                children.iter().all(|c| c.is_fully_compiled())
            }
            _ => true,
        }
    }

    /// Evaluate the compiled predicate against a row
    ///
    /// Returns true if the row matches the predicate, false otherwise.
    /// Returns None for NULL comparisons (three-valued logic).
    #[inline]
    pub fn evaluate(&self, row: &vibesql_storage::Row) -> Option<bool> {
        match self {
            CompiledPredicate::Equals { col_idx, value } => {
                let row_value = row.get(*col_idx)?;
                Some(Self::values_equal(row_value, value))
            }

            CompiledPredicate::NotEquals { col_idx, value } => {
                let row_value = row.get(*col_idx)?;
                // NULL != anything is NULL (unknown)
                if matches!(row_value, SqlValue::Null) || matches!(value, SqlValue::Null) {
                    return None;
                }
                Some(!Self::values_equal(row_value, value))
            }

            CompiledPredicate::Range { col_idx, op, value } => {
                let row_value = row.get(*col_idx)?;
                Self::compare_range(row_value, *op, value)
            }

            CompiledPredicate::IsNull { col_idx } => {
                let row_value = row.get(*col_idx)?;
                Some(matches!(row_value, SqlValue::Null))
            }

            CompiledPredicate::IsNotNull { col_idx } => {
                let row_value = row.get(*col_idx)?;
                Some(!matches!(row_value, SqlValue::Null))
            }

            CompiledPredicate::And(left, right) => {
                let left_result = left.evaluate(row);
                // Short-circuit: false AND anything = false
                if left_result == Some(false) {
                    return Some(false);
                }

                let right_result = right.evaluate(row);

                // SQL three-valued logic for AND:
                // false AND null = false
                // null AND false = false
                // true AND null = null
                // null AND true = null
                // null AND null = null
                match (left_result, right_result) {
                    (Some(true), Some(true)) => Some(true),
                    (Some(false), _) | (_, Some(false)) => Some(false),
                    _ => None, // At least one NULL and no false
                }
            }

            CompiledPredicate::Or(left, right) => {
                let left_result = left.evaluate(row);
                // Short-circuit: true OR anything = true
                if left_result == Some(true) {
                    return Some(true);
                }

                let right_result = right.evaluate(row);

                // SQL three-valued logic for OR:
                // true OR null = true
                // null OR true = true
                // false OR null = null
                // null OR false = null
                // null OR null = null
                match (left_result, right_result) {
                    (Some(true), _) | (_, Some(true)) => Some(true),
                    (Some(false), Some(false)) => Some(false),
                    _ => None, // At least one NULL and no true
                }
            }

            // N-ary conjunction (AND chain) with short-circuit evaluation
            CompiledPredicate::Conjunction(children) => {
                let mut has_null = false;
                for child in children.iter() {
                    match child.evaluate(row) {
                        Some(false) => return Some(false), // Short-circuit
                        Some(true) => {}
                        None => has_null = true,
                    }
                }
                // If any child was NULL and none were false, result is NULL
                if has_null {
                    None
                } else {
                    Some(true)
                }
            }

            // N-ary disjunction (OR chain) with short-circuit evaluation
            CompiledPredicate::Disjunction(children) => {
                let mut has_null = false;
                for child in children.iter() {
                    match child.evaluate(row) {
                        Some(true) => return Some(true), // Short-circuit
                        Some(false) => {}
                        None => has_null = true,
                    }
                }
                // If any child was NULL and none were true, result is NULL
                if has_null {
                    None
                } else {
                    Some(false)
                }
            }

            CompiledPredicate::Complex(_) => {
                // Cannot evaluate complex predicates with this fast path
                // This should not be called - caller should check is_fully_compiled first
                None
            }
        }
    }

    /// Compare two values for equality
    #[inline]
    fn values_equal(a: &SqlValue, b: &SqlValue) -> bool {
        // NULL = anything is false (not NULL)
        if matches!(a, SqlValue::Null) || matches!(b, SqlValue::Null) {
            return false;
        }

        // Fast path for common types
        match (a, b) {
            (SqlValue::Integer(x), SqlValue::Integer(y)) => x == y,
            (SqlValue::Bigint(x), SqlValue::Bigint(y)) => x == y,
            (SqlValue::Varchar(x), SqlValue::Varchar(y)) => x == y,
            (SqlValue::Boolean(x), SqlValue::Boolean(y)) => x == y,

            // Cross-type integer comparisons - promote Smallint to i64
            // Note: Integer and Bigint are both i64 internally
            (SqlValue::Integer(x), SqlValue::Bigint(y)) => x == y,
            (SqlValue::Bigint(x), SqlValue::Integer(y)) => x == y,
            (SqlValue::Integer(x), SqlValue::Smallint(y)) => *x == i64::from(*y),
            (SqlValue::Smallint(x), SqlValue::Integer(y)) => i64::from(*x) == *y,
            (SqlValue::Bigint(x), SqlValue::Smallint(y)) => *x == i64::from(*y),
            (SqlValue::Smallint(x), SqlValue::Bigint(y)) => i64::from(*x) == *y,

            // Floating point equality (same type)
            (SqlValue::Float(x), SqlValue::Float(y)) => x == y,
            (SqlValue::Double(x), SqlValue::Double(y)) => x == y,
            (SqlValue::Real(x), SqlValue::Real(y)) => x == y,
            (SqlValue::Numeric(x), SqlValue::Numeric(y)) => x == y,

            // Cross-type floating point equality - promote to f64 for comparison
            // Float <-> Numeric
            (SqlValue::Float(x), SqlValue::Numeric(y)) => f64::from(*x) == *y,
            (SqlValue::Numeric(x), SqlValue::Float(y)) => *x == f64::from(*y),
            // Float <-> Double
            (SqlValue::Float(x), SqlValue::Double(y)) => f64::from(*x) == *y,
            (SqlValue::Double(x), SqlValue::Float(y)) => *x == f64::from(*y),
            // Float <-> Real (Float is f32, Real is now f64)
            (SqlValue::Float(x), SqlValue::Real(y)) => f64::from(*x) == *y,
            (SqlValue::Real(x), SqlValue::Float(y)) => *x == f64::from(*y),
            // Double <-> Numeric (both f64)
            (SqlValue::Double(x), SqlValue::Numeric(y)) => x == y,
            (SqlValue::Numeric(x), SqlValue::Double(y)) => x == y,
            // Double <-> Real (both f64)
            (SqlValue::Double(x), SqlValue::Real(y)) => x == y,
            (SqlValue::Real(x), SqlValue::Double(y)) => x == y,
            // Real <-> Numeric (both f64)
            (SqlValue::Real(x), SqlValue::Numeric(y)) => x == y,
            (SqlValue::Numeric(x), SqlValue::Real(y)) => x == y,

            // Integer <-> Floating point equality (promote integers to f64)
            (SqlValue::Integer(x), SqlValue::Float(y)) => (*x as f64) == f64::from(*y),
            (SqlValue::Float(x), SqlValue::Integer(y)) => f64::from(*x) == (*y as f64),
            (SqlValue::Integer(x), SqlValue::Double(y)) => (*x as f64) == *y,
            (SqlValue::Double(x), SqlValue::Integer(y)) => *x == (*y as f64),
            (SqlValue::Integer(x), SqlValue::Numeric(y)) => (*x as f64) == *y,
            (SqlValue::Numeric(x), SqlValue::Integer(y)) => *x == (*y as f64),
            (SqlValue::Integer(x), SqlValue::Real(y)) => (*x as f64) == f64::from(*y),
            (SqlValue::Real(x), SqlValue::Integer(y)) => f64::from(*x) == (*y as f64),

            // Integer <-> String coercion (SQLite type affinity)
            // When comparing integer column to string literal, try to parse string as number
            (SqlValue::Integer(x), SqlValue::Varchar(s))
            | (SqlValue::Integer(x), SqlValue::Character(s)) => {
                s.trim().parse::<i64>().map(|y| *x == y).unwrap_or(false)
            }
            (SqlValue::Varchar(s), SqlValue::Integer(y))
            | (SqlValue::Character(s), SqlValue::Integer(y)) => {
                s.trim().parse::<i64>().map(|x| x == *y).unwrap_or(false)
            }
            // Bigint <-> String
            (SqlValue::Bigint(x), SqlValue::Varchar(s))
            | (SqlValue::Bigint(x), SqlValue::Character(s)) => {
                s.trim().parse::<i64>().map(|y| *x == y).unwrap_or(false)
            }
            (SqlValue::Varchar(s), SqlValue::Bigint(y))
            | (SqlValue::Character(s), SqlValue::Bigint(y)) => {
                s.trim().parse::<i64>().map(|x| x == *y).unwrap_or(false)
            }
            // Smallint <-> String
            (SqlValue::Smallint(x), SqlValue::Varchar(s))
            | (SqlValue::Smallint(x), SqlValue::Character(s)) => {
                s.trim().parse::<i16>().map(|y| *x == y).unwrap_or(false)
            }
            (SqlValue::Varchar(s), SqlValue::Smallint(y))
            | (SqlValue::Character(s), SqlValue::Smallint(y)) => {
                s.trim().parse::<i16>().map(|x| x == *y).unwrap_or(false)
            }
            // Float/Double/Real <-> String
            (SqlValue::Float(x), SqlValue::Varchar(s))
            | (SqlValue::Float(x), SqlValue::Character(s)) => {
                s.trim().parse::<f64>().map(|y| f64::from(*x) == y).unwrap_or(false)
            }
            (SqlValue::Varchar(s), SqlValue::Float(y))
            | (SqlValue::Character(s), SqlValue::Float(y)) => {
                s.trim().parse::<f64>().map(|x| x == f64::from(*y)).unwrap_or(false)
            }
            (SqlValue::Double(x), SqlValue::Varchar(s))
            | (SqlValue::Double(x), SqlValue::Character(s)) => {
                s.trim().parse::<f64>().map(|y| *x == y).unwrap_or(false)
            }
            (SqlValue::Varchar(s), SqlValue::Double(y))
            | (SqlValue::Character(s), SqlValue::Double(y)) => {
                s.trim().parse::<f64>().map(|x| x == *y).unwrap_or(false)
            }
            (SqlValue::Real(x), SqlValue::Varchar(s))
            | (SqlValue::Real(x), SqlValue::Character(s)) => {
                s.trim().parse::<f64>().map(|y| *x == y).unwrap_or(false)
            }
            (SqlValue::Varchar(s), SqlValue::Real(y))
            | (SqlValue::Character(s), SqlValue::Real(y)) => {
                s.trim().parse::<f64>().map(|x| x == *y).unwrap_or(false)
            }
            (SqlValue::Numeric(x), SqlValue::Varchar(s))
            | (SqlValue::Numeric(x), SqlValue::Character(s)) => {
                s.trim().parse::<f64>().map(|y| *x == y).unwrap_or(false)
            }
            (SqlValue::Varchar(s), SqlValue::Numeric(y))
            | (SqlValue::Character(s), SqlValue::Numeric(y)) => {
                s.trim().parse::<f64>().map(|x| x == *y).unwrap_or(false)
            }

            // Timestamp/Time <-> String: TEXT-rendering equality, matching
            // the expression evaluator's #5329 semantics (issue #5335: the
            // PartialEq fallback was always false, so `ts = '<rendering>'`
            // missed rows and `ts != 'junk'` matched nothing)
            (SqlValue::Timestamp(x), SqlValue::Varchar(s))
            | (SqlValue::Timestamp(x), SqlValue::Character(s)) => x.to_string() == s.as_str(),
            (SqlValue::Varchar(s), SqlValue::Timestamp(y))
            | (SqlValue::Character(s), SqlValue::Timestamp(y)) => y.to_string() == s.as_str(),
            (SqlValue::Time(x), SqlValue::Varchar(s))
            | (SqlValue::Time(x), SqlValue::Character(s)) => x.to_string() == s.as_str(),
            (SqlValue::Varchar(s), SqlValue::Time(y))
            | (SqlValue::Character(s), SqlValue::Time(y)) => y.to_string() == s.as_str(),

            // Date <-> String: parse-first (#5329). Unparseable strings are
            // declined at compile time (the evaluator raises a type-mismatch
            // error); defensively report not-equal if one slips through.
            (SqlValue::Date(x), SqlValue::Varchar(s))
            | (SqlValue::Date(x), SqlValue::Character(s)) => {
                use std::str::FromStr;
                vibesql_types::Date::from_str(s).map(|d| *x == d).unwrap_or(false)
            }
            (SqlValue::Varchar(s), SqlValue::Date(y))
            | (SqlValue::Character(s), SqlValue::Date(y)) => {
                use std::str::FromStr;
                vibesql_types::Date::from_str(s).map(|d| d == *y).unwrap_or(false)
            }

            // Fallback to PartialEq
            _ => a == b,
        }
    }

    /// Compare a row value against a literal using a range operator
    #[inline]
    fn compare_range(row_value: &SqlValue, op: RangeOp, literal: &SqlValue) -> Option<bool> {
        // NULL comparisons return NULL (unknown)
        if matches!(row_value, SqlValue::Null) || matches!(literal, SqlValue::Null) {
            return None;
        }

        // Fast path for common types
        match (row_value, literal) {
            (SqlValue::Integer(x), SqlValue::Integer(y)) => Some(Self::apply_range_op(*x, op, *y)),
            (SqlValue::Bigint(x), SqlValue::Bigint(y)) => Some(Self::apply_range_op(*x, op, *y)),
            (SqlValue::Smallint(x), SqlValue::Smallint(y)) => {
                Some(Self::apply_range_op(*x, op, *y))
            }

            // Cross-type integer comparisons - promote Smallint to i64
            // Note: Integer and Bigint are both i64 internally
            (SqlValue::Integer(x), SqlValue::Smallint(y)) => {
                Some(Self::apply_range_op(*x, op, i64::from(*y)))
            }
            (SqlValue::Smallint(x), SqlValue::Integer(y)) => {
                Some(Self::apply_range_op(i64::from(*x), op, *y))
            }
            // Bigint and Smallint
            (SqlValue::Bigint(x), SqlValue::Smallint(y)) => {
                Some(Self::apply_range_op(*x, op, i64::from(*y)))
            }
            (SqlValue::Smallint(x), SqlValue::Bigint(y)) => {
                Some(Self::apply_range_op(i64::from(*x), op, *y))
            }
            // Integer and Bigint are the same type (i64), but keep for explicitness
            (SqlValue::Integer(x), SqlValue::Bigint(y)) => Some(Self::apply_range_op(*x, op, *y)),
            (SqlValue::Bigint(x), SqlValue::Integer(y)) => Some(Self::apply_range_op(*x, op, *y)),

            // String comparisons
            (SqlValue::Varchar(x), SqlValue::Varchar(y)) => {
                Some(Self::apply_range_op(&**x, op, &**y))
            }

            // Floating point comparisons (same type)
            (SqlValue::Float(x), SqlValue::Float(y)) => Some(Self::apply_range_op(*x, op, *y)),
            (SqlValue::Double(x), SqlValue::Double(y)) => Some(Self::apply_range_op(*x, op, *y)),
            (SqlValue::Real(x), SqlValue::Real(y)) => Some(Self::apply_range_op(*x, op, *y)),
            (SqlValue::Numeric(x), SqlValue::Numeric(y)) => Some(Self::apply_range_op(*x, op, *y)),

            // Cross-type floating point comparisons - promote to f64 for accurate comparison
            // Float <-> Numeric
            (SqlValue::Float(x), SqlValue::Numeric(y)) => {
                Some(Self::apply_range_op(f64::from(*x), op, *y))
            }
            (SqlValue::Numeric(x), SqlValue::Float(y)) => {
                Some(Self::apply_range_op(*x, op, f64::from(*y)))
            }
            // Float <-> Double
            (SqlValue::Float(x), SqlValue::Double(y)) => {
                Some(Self::apply_range_op(f64::from(*x), op, *y))
            }
            (SqlValue::Double(x), SqlValue::Float(y)) => {
                Some(Self::apply_range_op(*x, op, f64::from(*y)))
            }
            // Float <-> Real (Float is f32, Real is now f64)
            (SqlValue::Float(x), SqlValue::Real(y)) => {
                Some(Self::apply_range_op(f64::from(*x), op, *y))
            }
            (SqlValue::Real(x), SqlValue::Float(y)) => {
                Some(Self::apply_range_op(*x, op, f64::from(*y)))
            }
            // Double <-> Numeric (both f64)
            (SqlValue::Double(x), SqlValue::Numeric(y)) => Some(Self::apply_range_op(*x, op, *y)),
            (SqlValue::Numeric(x), SqlValue::Double(y)) => Some(Self::apply_range_op(*x, op, *y)),
            // Double <-> Real (both f64)
            (SqlValue::Double(x), SqlValue::Real(y)) => Some(Self::apply_range_op(*x, op, *y)),
            (SqlValue::Real(x), SqlValue::Double(y)) => Some(Self::apply_range_op(*x, op, *y)),
            // Real <-> Numeric (both f64)
            (SqlValue::Real(x), SqlValue::Numeric(y)) => Some(Self::apply_range_op(*x, op, *y)),
            (SqlValue::Numeric(x), SqlValue::Real(y)) => Some(Self::apply_range_op(*x, op, *y)),

            // Cross-type Float/Double/Real vs Integer comparisons - promote to f64
            // This fixes issue #3360: Float(678.28) > Integer(85) was returning None
            (SqlValue::Float(x), SqlValue::Integer(y)) => {
                Some(Self::apply_range_op(f64::from(*x), op, *y as f64))
            }
            (SqlValue::Integer(x), SqlValue::Float(y)) => {
                Some(Self::apply_range_op(*x as f64, op, f64::from(*y)))
            }
            (SqlValue::Double(x), SqlValue::Integer(y)) => {
                Some(Self::apply_range_op(*x, op, *y as f64))
            }
            (SqlValue::Integer(x), SqlValue::Double(y)) => {
                Some(Self::apply_range_op(*x as f64, op, *y))
            }
            (SqlValue::Real(x), SqlValue::Integer(y)) => {
                Some(Self::apply_range_op(f64::from(*x), op, *y as f64))
            }
            (SqlValue::Integer(x), SqlValue::Real(y)) => {
                Some(Self::apply_range_op(*x as f64, op, f64::from(*y)))
            }
            (SqlValue::Numeric(x), SqlValue::Integer(y)) => {
                Some(Self::apply_range_op(*x, op, *y as f64))
            }
            (SqlValue::Integer(x), SqlValue::Numeric(y)) => {
                Some(Self::apply_range_op(*x as f64, op, *y))
            }

            // Float/Double/Real/Numeric vs Bigint comparisons
            (SqlValue::Float(x), SqlValue::Bigint(y)) => {
                Some(Self::apply_range_op(f64::from(*x), op, *y as f64))
            }
            (SqlValue::Bigint(x), SqlValue::Float(y)) => {
                Some(Self::apply_range_op(*x as f64, op, f64::from(*y)))
            }
            (SqlValue::Double(x), SqlValue::Bigint(y)) => {
                Some(Self::apply_range_op(*x, op, *y as f64))
            }
            (SqlValue::Bigint(x), SqlValue::Double(y)) => {
                Some(Self::apply_range_op(*x as f64, op, *y))
            }
            (SqlValue::Real(x), SqlValue::Bigint(y)) => {
                Some(Self::apply_range_op(f64::from(*x), op, *y as f64))
            }
            (SqlValue::Bigint(x), SqlValue::Real(y)) => {
                Some(Self::apply_range_op(*x as f64, op, f64::from(*y)))
            }
            (SqlValue::Numeric(x), SqlValue::Bigint(y)) => {
                Some(Self::apply_range_op(*x, op, *y as f64))
            }
            (SqlValue::Bigint(x), SqlValue::Numeric(y)) => {
                Some(Self::apply_range_op(*x as f64, op, *y))
            }

            // Float/Double/Real/Numeric vs Smallint comparisons
            (SqlValue::Float(x), SqlValue::Smallint(y)) => {
                Some(Self::apply_range_op(*x, op, f32::from(*y)))
            }
            (SqlValue::Smallint(x), SqlValue::Float(y)) => {
                Some(Self::apply_range_op(f32::from(*x), op, *y))
            }
            (SqlValue::Double(x), SqlValue::Smallint(y)) => {
                Some(Self::apply_range_op(*x, op, f64::from(*y)))
            }
            (SqlValue::Smallint(x), SqlValue::Double(y)) => {
                Some(Self::apply_range_op(f64::from(*x), op, *y))
            }
            (SqlValue::Real(x), SqlValue::Smallint(y)) => {
                Some(Self::apply_range_op(*x, op, f64::from(*y))) // Real is now f64
            }
            (SqlValue::Smallint(x), SqlValue::Real(y)) => {
                Some(Self::apply_range_op(f64::from(*x), op, *y)) // Real is now f64
            }
            (SqlValue::Numeric(x), SqlValue::Smallint(y)) => {
                Some(Self::apply_range_op(*x, op, f64::from(*y)))
            }
            (SqlValue::Smallint(x), SqlValue::Numeric(y)) => {
                Some(Self::apply_range_op(f64::from(*x), op, *y))
            }

            // Integer <-> String range comparisons (SQLite type affinity)
            // When comparing integer column to string literal, try to parse string as number
            (SqlValue::Integer(x), SqlValue::Varchar(s))
            | (SqlValue::Integer(x), SqlValue::Character(s)) => {
                s.trim().parse::<i64>().map(|y| Self::apply_range_op(*x, op, y)).ok()
            }
            (SqlValue::Varchar(s), SqlValue::Integer(y))
            | (SqlValue::Character(s), SqlValue::Integer(y)) => {
                s.trim().parse::<i64>().map(|x| Self::apply_range_op(x, op, *y)).ok()
            }
            // Bigint <-> String
            (SqlValue::Bigint(x), SqlValue::Varchar(s))
            | (SqlValue::Bigint(x), SqlValue::Character(s)) => {
                s.trim().parse::<i64>().map(|y| Self::apply_range_op(*x, op, y)).ok()
            }
            (SqlValue::Varchar(s), SqlValue::Bigint(y))
            | (SqlValue::Character(s), SqlValue::Bigint(y)) => {
                s.trim().parse::<i64>().map(|x| Self::apply_range_op(x, op, *y)).ok()
            }
            // Float/Double/Real <-> String
            (SqlValue::Float(x), SqlValue::Varchar(s))
            | (SqlValue::Float(x), SqlValue::Character(s)) => {
                s.trim().parse::<f64>().map(|y| Self::apply_range_op(f64::from(*x), op, y)).ok()
            }
            (SqlValue::Varchar(s), SqlValue::Float(y))
            | (SqlValue::Character(s), SqlValue::Float(y)) => {
                s.trim().parse::<f64>().map(|x| Self::apply_range_op(x, op, f64::from(*y))).ok()
            }
            (SqlValue::Double(x), SqlValue::Varchar(s))
            | (SqlValue::Double(x), SqlValue::Character(s)) => {
                s.trim().parse::<f64>().map(|y| Self::apply_range_op(*x, op, y)).ok()
            }
            (SqlValue::Varchar(s), SqlValue::Double(y))
            | (SqlValue::Character(s), SqlValue::Double(y)) => {
                s.trim().parse::<f64>().map(|x| Self::apply_range_op(x, op, *y)).ok()
            }
            (SqlValue::Real(x), SqlValue::Varchar(s))
            | (SqlValue::Real(x), SqlValue::Character(s)) => {
                s.trim().parse::<f64>().map(|y| Self::apply_range_op(*x, op, y)).ok()
            }
            (SqlValue::Varchar(s), SqlValue::Real(y))
            | (SqlValue::Character(s), SqlValue::Real(y)) => {
                s.trim().parse::<f64>().map(|x| Self::apply_range_op(x, op, *y)).ok()
            }

            // Same-type temporal comparisons (issue #5335: previously fell
            // through to the None fallback, which callers treat as exclude)
            (SqlValue::Date(x), SqlValue::Date(y)) => Some(Self::apply_range_op(x, op, y)),
            (SqlValue::Time(x), SqlValue::Time(y)) => Some(Self::apply_range_op(x, op, y)),
            (SqlValue::Timestamp(x), SqlValue::Timestamp(y)) => {
                Some(Self::apply_range_op(x, op, y))
            }

            // Timestamp/Time <-> String: compare TEXT renderings
            // lexicographically, matching the expression evaluator's #5329
            // semantics (e.g. `ts < 'hello'` is true for every timestamp
            // because renderings start with a digit)
            (SqlValue::Timestamp(x), SqlValue::Varchar(s))
            | (SqlValue::Timestamp(x), SqlValue::Character(s)) => {
                Some(Self::apply_range_op(x.to_string().as_str(), op, s.as_str()))
            }
            (SqlValue::Varchar(s), SqlValue::Timestamp(y))
            | (SqlValue::Character(s), SqlValue::Timestamp(y)) => {
                Some(Self::apply_range_op(s.as_str(), op, y.to_string().as_str()))
            }
            (SqlValue::Time(x), SqlValue::Varchar(s))
            | (SqlValue::Time(x), SqlValue::Character(s)) => {
                Some(Self::apply_range_op(x.to_string().as_str(), op, s.as_str()))
            }
            (SqlValue::Varchar(s), SqlValue::Time(y))
            | (SqlValue::Character(s), SqlValue::Time(y)) => {
                Some(Self::apply_range_op(s.as_str(), op, y.to_string().as_str()))
            }

            // Date <-> String: parse-first (#5329). Unparseable strings are
            // declined at compile time; None (exclude) if one slips through.
            (SqlValue::Date(x), SqlValue::Varchar(s))
            | (SqlValue::Date(x), SqlValue::Character(s)) => {
                use std::str::FromStr;
                vibesql_types::Date::from_str(s).ok().map(|d| Self::apply_range_op(*x, op, d))
            }
            (SqlValue::Varchar(s), SqlValue::Date(y))
            | (SqlValue::Character(s), SqlValue::Date(y)) => {
                use std::str::FromStr;
                vibesql_types::Date::from_str(s).ok().map(|d| Self::apply_range_op(d, op, *y))
            }

            // BLOB vs BLOB - bytewise comparison (SQLite memcmp semantics)
            (SqlValue::Blob(x), SqlValue::Blob(y)) => Some(Self::apply_range_op(x, op, y)),

            // BLOB vs TEXT - SQLite type ordering: TEXT < BLOB (no coercion)
            (SqlValue::Blob(_), SqlValue::Varchar(_) | SqlValue::Character(_)) => {
                // BLOB > TEXT, so any > / >= is true; any < / <= is false
                Some(matches!(op, RangeOp::GreaterThan | RangeOp::GreaterThanOrEqual))
            }
            (SqlValue::Varchar(_) | SqlValue::Character(_), SqlValue::Blob(_)) => {
                // TEXT < BLOB, so any < / <= is true; any > / >= is false
                Some(matches!(op, RangeOp::LessThan | RangeOp::LessThanOrEqual))
            }

            // BLOB vs Numeric - SQLite type ordering: Numeric < BLOB (no coercion)
            (
                SqlValue::Blob(_),
                SqlValue::Integer(_)
                | SqlValue::Smallint(_)
                | SqlValue::Bigint(_)
                | SqlValue::Float(_)
                | SqlValue::Real(_)
                | SqlValue::Double(_)
                | SqlValue::Numeric(_),
            ) => Some(matches!(op, RangeOp::GreaterThan | RangeOp::GreaterThanOrEqual)),
            (
                SqlValue::Integer(_)
                | SqlValue::Smallint(_)
                | SqlValue::Bigint(_)
                | SqlValue::Float(_)
                | SqlValue::Real(_)
                | SqlValue::Double(_)
                | SqlValue::Numeric(_),
                SqlValue::Blob(_),
            ) => Some(matches!(op, RangeOp::LessThan | RangeOp::LessThanOrEqual)),

            // Type mismatch - fall back to None (needs full evaluation)
            _ => None,
        }
    }

    /// Apply a range operator to two comparable values
    #[inline]
    fn apply_range_op<T: PartialOrd>(left: T, op: RangeOp, right: T) -> bool {
        match op {
            RangeOp::LessThan => left < right,
            RangeOp::LessThanOrEqual => left <= right,
            RangeOp::GreaterThan => left > right,
            RangeOp::GreaterThanOrEqual => left >= right,
        }
    }
}

#[cfg(test)]
mod tests {
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_storage::Row;
    use vibesql_types::{DataType, SqlValue};

    use super::*;

    fn create_test_schema() -> CombinedSchema {
        let columns = vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(255) },
                true,
            ),
        ];
        let schema = TableSchema::new("test".to_string(), columns);
        CombinedSchema::from_table("test".to_string(), schema)
    }

    #[test]
    fn test_compile_simple_equals() {
        let schema = create_test_schema();
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                "id", false,
            ))),
            op: BinaryOperator::Equal,
            right: Box::new(Expression::Literal(SqlValue::Integer(42))),
        };

        let compiled = CompiledPredicate::compile(&expr, &schema);
        assert!(compiled.is_fully_compiled());

        if let CompiledPredicate::Equals { col_idx, value } = compiled {
            assert_eq!(col_idx, 0);
            assert_eq!(value, SqlValue::Integer(42));
        } else {
            panic!("Expected Equals predicate");
        }
    }

    /// Schema mirroring `in4.test`'s t4a: `a` has default BINARY collation,
    /// `b` is declared NOCASE (issue #5792).
    fn create_collated_schema() -> CombinedSchema {
        let columns = vec![
            ColumnSchema::new("a".to_string(), DataType::Varchar { max_length: None }, true),
            ColumnSchema {
                name: "b".to_string(),
                data_type: DataType::Varchar { max_length: None },
                nullable: true,
                default_value: None,
                generated_expr: None,
                is_exact_integer_type: false,
                collation: Some("NOCASE".to_string()),
            },
        ];
        let schema = TableSchema::new("t4a".to_string(), columns);
        CombinedSchema::from_table("t4a".to_string(), schema)
    }

    #[test]
    fn test_collated_column_comparison_declined() {
        // Issue #5792: comparisons against a NOCASE column must not compile;
        // the fast path compares raw values and would miss 'XYZ' = 'xyz'.
        let schema = create_collated_schema();
        for (left, right) in [
            (
                Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("b", false)),
                Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("xyz"))),
            ),
            (
                Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("xyz"))),
                Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("b", false)),
            ),
        ] {
            let expr = Expression::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::Equal,
                right: Box::new(right),
            };
            let compiled = CompiledPredicate::compile(&expr, &schema);
            assert!(
                !compiled.is_fully_compiled(),
                "NOCASE column comparison must fall back to the expression evaluator"
            );
        }
    }

    #[test]
    fn test_binary_collated_column_still_compiles() {
        // A column with no declared collation (default BINARY) keeps the
        // fast path.
        let schema = create_collated_schema();
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                "a", false,
            ))),
            op: BinaryOperator::Equal,
            right: Box::new(Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("ABC")))),
        };
        let compiled = CompiledPredicate::compile(&expr, &schema);
        assert!(compiled.is_fully_compiled());
    }

    #[test]
    fn test_evaluate_equals() {
        let schema = create_test_schema();
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                "id", false,
            ))),
            op: BinaryOperator::Equal,
            right: Box::new(Expression::Literal(SqlValue::Integer(42))),
        };

        let compiled = CompiledPredicate::compile(&expr, &schema);

        // Test matching row
        let row = Row::from_vec(vec![
            SqlValue::Integer(42),
            SqlValue::Varchar(arcstr::ArcStr::from("test")),
        ]);
        assert_eq!(compiled.evaluate(&row), Some(true));

        // Test non-matching row
        let row = Row::from_vec(vec![
            SqlValue::Integer(99),
            SqlValue::Varchar(arcstr::ArcStr::from("test")),
        ]);
        assert_eq!(compiled.evaluate(&row), Some(false));
    }

    /// Schema with a single TEXT-affinity column `a` (matches `indexA.test`'s
    /// `CREATE TABLE x1(a TEXT, ...)`).
    fn create_text_col_schema() -> CombinedSchema {
        let columns =
            vec![ColumnSchema::new("a".to_string(), DataType::Varchar { max_length: None }, true)];
        let schema = TableSchema::new("x1".to_string(), columns);
        CombinedSchema::from_table("x1".to_string(), schema)
    }

    /// Schema with a single NUMERIC-affinity column `a`
    /// (matches `indexA.test`'s `CREATE TABLE x2(a NUMERIC, ...)`).
    fn create_numeric_col_schema() -> CombinedSchema {
        let columns = vec![ColumnSchema::new(
            "a".to_string(),
            DataType::Numeric { precision: 10, scale: 0 },
            true,
        )];
        let schema = TableSchema::new("x2".to_string(), columns);
        CombinedSchema::from_table("x2".to_string(), schema)
    }

    fn col_op_lit(op: BinaryOperator, lit: SqlValue) -> Expression {
        Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                "a", false,
            ))),
            op,
            right: Box::new(Expression::Literal(lit)),
        }
    }

    fn lit_op_col(lit: SqlValue, op: BinaryOperator) -> Expression {
        Expression::BinaryOp {
            left: Box::new(Expression::Literal(lit)),
            op,
            right: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                "a", false,
            ))),
        }
    }

    fn text_row(s: &str) -> Row {
        Row::from_vec(vec![SqlValue::Varchar(arcstr::ArcStr::from(s))])
    }

    /// Issue #5765: a TEXT-affinity column compared to a numeric literal must
    /// apply TEXT affinity to the literal and compare as strings, so the
    /// compiled fast path matches `apply_affinity_for_comparison` Case 1 and
    /// the SQLite `indexA.test` expectations (`SELECT ... WHERE a=2` on a TEXT
    /// column returns only the `'2'` row, not `'2.0'`).
    #[test]
    fn test_text_col_vs_numeric_literal_affinity() {
        let schema = create_text_col_schema();

        // a = 2 (INTEGER literal) -> coerce literal to '2', string compare.
        let expr = col_op_lit(BinaryOperator::Equal, SqlValue::Integer(2));
        let compiled = CompiledPredicate::compile(&expr, &schema);
        assert!(
            compiled.is_fully_compiled(),
            "TEXT col = INTEGER must stay on the compiled fast path"
        );
        // The literal must have been rendered to text at compile time.
        if let CompiledPredicate::Equals { value, .. } = &compiled {
            assert_eq!(*value, SqlValue::Varchar(arcstr::ArcStr::from("2")));
        } else {
            panic!("expected Equals predicate");
        }
        assert_eq!(compiled.evaluate(&text_row("2")), Some(true), "'2' = 2 -> true");
        assert_eq!(compiled.evaluate(&text_row("2.0")), Some(false), "'2.0' = 2 -> false");

        // a = 2.0 (REAL literal) -> coerce literal to '2.0', string compare.
        let expr = col_op_lit(BinaryOperator::Equal, SqlValue::Real(2.0));
        let compiled = CompiledPredicate::compile(&expr, &schema);
        if let CompiledPredicate::Equals { value, .. } = &compiled {
            assert_eq!(*value, SqlValue::Varchar(arcstr::ArcStr::from("2.0")));
        } else {
            panic!("expected Equals predicate");
        }
        assert_eq!(compiled.evaluate(&text_row("2")), Some(false), "'2' = 2.0 -> false");
        assert_eq!(compiled.evaluate(&text_row("2.0")), Some(true), "'2.0' = 2.0 -> true");
    }

    /// Symmetric form: numeric literal on the left (`2 = a`) must coerce the
    /// same way as `a = 2`.
    #[test]
    fn test_text_col_vs_numeric_literal_symmetric() {
        let schema = create_text_col_schema();

        let expr = lit_op_col(SqlValue::Integer(2), BinaryOperator::Equal);
        let compiled = CompiledPredicate::compile(&expr, &schema);
        assert!(compiled.is_fully_compiled());
        assert_eq!(compiled.evaluate(&text_row("2")), Some(true), "2 = '2' -> true");
        assert_eq!(compiled.evaluate(&text_row("2.0")), Some(false), "2 = '2.0' -> false");
    }

    /// NotEqual must be the logical negation of Equal under TEXT affinity.
    #[test]
    fn test_text_col_vs_numeric_literal_not_equal() {
        let schema = create_text_col_schema();

        let expr = col_op_lit(BinaryOperator::NotEqual, SqlValue::Integer(2));
        let compiled = CompiledPredicate::compile(&expr, &schema);
        assert!(compiled.is_fully_compiled());
        assert_eq!(compiled.evaluate(&text_row("2")), Some(false), "'2' <> 2 -> false");
        assert_eq!(compiled.evaluate(&text_row("2.0")), Some(true), "'2.0' <> 2 -> true");
    }

    /// Float text representation must be preserved: TEXT '10' does NOT equal
    /// REAL 10.0 (rendered as '10.0'), but TEXT '10.0' does.
    #[test]
    fn test_text_col_vs_real_preserves_decimal_point() {
        let schema = create_text_col_schema();

        let expr = col_op_lit(BinaryOperator::Equal, SqlValue::Real(10.0));
        let compiled = CompiledPredicate::compile(&expr, &schema);
        if let CompiledPredicate::Equals { value, .. } = &compiled {
            assert_eq!(*value, SqlValue::Varchar(arcstr::ArcStr::from("10.0")));
        } else {
            panic!("expected Equals predicate");
        }
        assert_eq!(compiled.evaluate(&text_row("10")), Some(false), "'10' = 10.0 -> false");
        assert_eq!(compiled.evaluate(&text_row("10.0")), Some(true), "'10.0' = 10.0 -> true");
    }

    /// Regression guard: NUMERIC-affinity columns must NOT have the literal
    /// coerced to text — they keep numeric comparison semantics (so `'2.0' = 2`
    /// matches numerically, matching `indexA.test` x2 expectations).
    #[test]
    fn test_numeric_col_literal_not_coerced() {
        let schema = create_numeric_col_schema();
        let value = CompiledPredicate::coerce_literal_for_column(&schema, 0, SqlValue::Integer(2));
        assert_eq!(value, SqlValue::Integer(2), "NUMERIC column literal must stay numeric");

        let value = CompiledPredicate::coerce_literal_for_column(&schema, 0, SqlValue::Real(2.0));
        assert_eq!(value, SqlValue::Real(2.0), "NUMERIC column REAL literal must stay numeric");
    }

    #[test]
    fn test_evaluate_and() {
        let schema = create_test_schema();
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                    "id", false,
                ))),
                op: BinaryOperator::GreaterThan,
                right: Box::new(Expression::Literal(SqlValue::Integer(10))),
            }),
            op: BinaryOperator::And,
            right: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                    "id", false,
                ))),
                op: BinaryOperator::LessThan,
                right: Box::new(Expression::Literal(SqlValue::Integer(100))),
            }),
        };

        let compiled = CompiledPredicate::compile(&expr, &schema);
        assert!(compiled.is_fully_compiled());

        // Test row that matches both conditions
        let row = Row::from_vec(vec![
            SqlValue::Integer(50),
            SqlValue::Varchar(arcstr::ArcStr::from("test")),
        ]);
        assert_eq!(compiled.evaluate(&row), Some(true));

        // Test row that fails first condition
        let row = Row::from_vec(vec![
            SqlValue::Integer(5),
            SqlValue::Varchar(arcstr::ArcStr::from("test")),
        ]);
        assert_eq!(compiled.evaluate(&row), Some(false));

        // Test row that fails second condition
        let row = Row::from_vec(vec![
            SqlValue::Integer(150),
            SqlValue::Varchar(arcstr::ArcStr::from("test")),
        ]);
        assert_eq!(compiled.evaluate(&row), Some(false));
    }

    /// Test for issue #3360: Float column vs Integer literal comparison
    /// This ensures that cross-type Float/Integer comparisons work correctly
    /// in the compiled predicate fast path.
    #[test]
    fn test_float_vs_integer_range_comparison() {
        // Test Float > Integer
        let result = CompiledPredicate::compare_range(
            &SqlValue::Float(678.28),
            RangeOp::GreaterThan,
            &SqlValue::Integer(85),
        );
        assert_eq!(result, Some(true), "Float(678.28) > Integer(85) should be true");

        let result = CompiledPredicate::compare_range(
            &SqlValue::Float(50.0),
            RangeOp::GreaterThan,
            &SqlValue::Integer(85),
        );
        assert_eq!(result, Some(false), "Float(50.0) > Integer(85) should be false");

        // Test Integer < Float
        let result = CompiledPredicate::compare_range(
            &SqlValue::Integer(85),
            RangeOp::LessThan,
            &SqlValue::Float(678.28),
        );
        assert_eq!(result, Some(true), "Integer(85) < Float(678.28) should be true");

        // Test Double vs Integer
        let result = CompiledPredicate::compare_range(
            &SqlValue::Double(678.28),
            RangeOp::GreaterThan,
            &SqlValue::Integer(85),
        );
        assert_eq!(result, Some(true), "Double(678.28) > Integer(85) should be true");

        // Test Real vs Integer
        let result = CompiledPredicate::compare_range(
            &SqlValue::Real(678.28),
            RangeOp::GreaterThan,
            &SqlValue::Integer(85),
        );
        assert_eq!(result, Some(true), "Real(678.28) > Integer(85) should be true");
    }

    #[test]
    fn test_compare_range_blob_vs_blob() {
        // BLOB vs BLOB - bytewise comparison (SQLite memcmp semantics)
        // Verified: SELECT x'616263' >= x'6162' → 1
        let abc = SqlValue::Blob(vec![0x61, 0x62, 0x63]);
        let ab = SqlValue::Blob(vec![0x61, 0x62]);
        assert_eq!(
            CompiledPredicate::compare_range(&abc, RangeOp::GreaterThanOrEqual, &ab),
            Some(true)
        );
        assert_eq!(CompiledPredicate::compare_range(&ab, RangeOp::LessThan, &abc), Some(true));
        assert_eq!(CompiledPredicate::compare_range(&abc, RangeOp::LessThan, &ab), Some(false));
    }

    #[test]
    fn test_compare_range_blob_vs_text() {
        // SQLite type ordering: TEXT < BLOB
        // Verified: SELECT 'abc' >= x'6162' → 0; SELECT x'616263' >= 'abc' → 1
        let blob = SqlValue::Blob(vec![0x61, 0x62]);
        let text = SqlValue::Varchar(arcstr::ArcStr::from("abc"));

        // BLOB > TEXT
        assert_eq!(
            CompiledPredicate::compare_range(&blob, RangeOp::GreaterThan, &text),
            Some(true)
        );
        assert_eq!(
            CompiledPredicate::compare_range(&blob, RangeOp::GreaterThanOrEqual, &text),
            Some(true)
        );
        assert_eq!(CompiledPredicate::compare_range(&blob, RangeOp::LessThan, &text), Some(false));
        // TEXT < BLOB
        assert_eq!(CompiledPredicate::compare_range(&text, RangeOp::LessThan, &blob), Some(true));
        assert_eq!(
            CompiledPredicate::compare_range(&text, RangeOp::GreaterThanOrEqual, &blob),
            Some(false)
        );
    }

    #[test]
    fn test_compare_range_blob_vs_numeric() {
        // SQLite type ordering: Numeric < BLOB (no coercion across storage classes)
        let blob = SqlValue::Blob(vec![0x00]);
        // BLOB > Integer
        assert_eq!(
            CompiledPredicate::compare_range(&blob, RangeOp::GreaterThan, &SqlValue::Integer(99)),
            Some(true)
        );
        // Integer < BLOB
        assert_eq!(
            CompiledPredicate::compare_range(&SqlValue::Integer(99), RangeOp::LessThan, &blob),
            Some(true)
        );
        // Real < BLOB
        assert_eq!(
            CompiledPredicate::compare_range(&SqlValue::Real(0.5), RangeOp::LessThanOrEqual, &blob),
            Some(true)
        );
        // BLOB not less than Numeric
        assert_eq!(
            CompiledPredicate::compare_range(&blob, RangeOp::LessThan, &SqlValue::Real(0.5)),
            Some(false)
        );
    }
}
