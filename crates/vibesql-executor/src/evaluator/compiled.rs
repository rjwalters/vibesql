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

use crate::schema::CombinedSchema;
use vibesql_ast::{BinaryOperator, Expression};
use vibesql_types::SqlValue;

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
                if let Expression::ColumnRef { table, column } = expr.as_ref() {
                    let col_idx = schema.get_column_index(table.as_deref(), column)?;
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
        if let (Expression::ColumnRef { table, column }, Expression::Literal(value)) = (left, right)
        {
            let col_idx = schema.get_column_index(table.as_deref(), column)?;
            return Self::compile_comparison_with_idx(col_idx, op, value.clone(), false);
        }

        // Try literal <op> col (reverse the operator)
        if let (Expression::Literal(value), Expression::ColumnRef { table, column }) = (left, right)
        {
            let col_idx = schema.get_column_index(table.as_deref(), column)?;
            return Self::compile_comparison_with_idx(col_idx, op, value.clone(), true);
        }

        None
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
            // Float <-> Real (both f32)
            (SqlValue::Float(x), SqlValue::Real(y)) => x == y,
            (SqlValue::Real(x), SqlValue::Float(y)) => x == y,
            // Double <-> Numeric (both f64)
            (SqlValue::Double(x), SqlValue::Numeric(y)) => x == y,
            (SqlValue::Numeric(x), SqlValue::Double(y)) => x == y,
            // Double <-> Real
            (SqlValue::Double(x), SqlValue::Real(y)) => *x == f64::from(*y),
            (SqlValue::Real(x), SqlValue::Double(y)) => f64::from(*x) == *y,
            // Real <-> Numeric
            (SqlValue::Real(x), SqlValue::Numeric(y)) => f64::from(*x) == *y,
            (SqlValue::Numeric(x), SqlValue::Real(y)) => *x == f64::from(*y),

            // Integer <-> Floating point equality (promote integers to f64)
            (SqlValue::Integer(x), SqlValue::Float(y)) => (*x as f64) == f64::from(*y),
            (SqlValue::Float(x), SqlValue::Integer(y)) => f64::from(*x) == (*y as f64),
            (SqlValue::Integer(x), SqlValue::Double(y)) => (*x as f64) == *y,
            (SqlValue::Double(x), SqlValue::Integer(y)) => *x == (*y as f64),
            (SqlValue::Integer(x), SqlValue::Numeric(y)) => (*x as f64) == *y,
            (SqlValue::Numeric(x), SqlValue::Integer(y)) => *x == (*y as f64),
            (SqlValue::Integer(x), SqlValue::Real(y)) => (*x as f64) == f64::from(*y),
            (SqlValue::Real(x), SqlValue::Integer(y)) => f64::from(*x) == (*y as f64),

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
            // Float <-> Real (both f32)
            (SqlValue::Float(x), SqlValue::Real(y)) => Some(Self::apply_range_op(*x, op, *y)),
            (SqlValue::Real(x), SqlValue::Float(y)) => Some(Self::apply_range_op(*x, op, *y)),
            // Double <-> Numeric (both f64)
            (SqlValue::Double(x), SqlValue::Numeric(y)) => Some(Self::apply_range_op(*x, op, *y)),
            (SqlValue::Numeric(x), SqlValue::Double(y)) => Some(Self::apply_range_op(*x, op, *y)),
            // Double <-> Real
            (SqlValue::Double(x), SqlValue::Real(y)) => {
                Some(Self::apply_range_op(*x, op, f64::from(*y)))
            }
            (SqlValue::Real(x), SqlValue::Double(y)) => {
                Some(Self::apply_range_op(f64::from(*x), op, *y))
            }
            // Real <-> Numeric
            (SqlValue::Real(x), SqlValue::Numeric(y)) => {
                Some(Self::apply_range_op(f64::from(*x), op, *y))
            }
            (SqlValue::Numeric(x), SqlValue::Real(y)) => {
                Some(Self::apply_range_op(*x, op, f64::from(*y)))
            }

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
                Some(Self::apply_range_op(*x, op, f32::from(*y)))
            }
            (SqlValue::Smallint(x), SqlValue::Real(y)) => {
                Some(Self::apply_range_op(f32::from(*x), op, *y))
            }
            (SqlValue::Numeric(x), SqlValue::Smallint(y)) => {
                Some(Self::apply_range_op(*x, op, f64::from(*y)))
            }
            (SqlValue::Smallint(x), SqlValue::Numeric(y)) => {
                Some(Self::apply_range_op(f64::from(*x), op, *y))
            }

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
    use super::*;
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_storage::Row;
    use vibesql_types::{DataType, SqlValue};

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
            left: Box::new(Expression::ColumnRef { table: None, column: "id".to_string() }),
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

    #[test]
    fn test_evaluate_equals() {
        let schema = create_test_schema();
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "id".to_string() }),
            op: BinaryOperator::Equal,
            right: Box::new(Expression::Literal(SqlValue::Integer(42))),
        };

        let compiled = CompiledPredicate::compile(&expr, &schema);

        // Test matching row
        let row =
            Row::from_vec(vec![SqlValue::Integer(42), SqlValue::Varchar(arcstr::ArcStr::from("test"))]);
        assert_eq!(compiled.evaluate(&row), Some(true));

        // Test non-matching row
        let row =
            Row::from_vec(vec![SqlValue::Integer(99), SqlValue::Varchar(arcstr::ArcStr::from("test"))]);
        assert_eq!(compiled.evaluate(&row), Some(false));
    }

    #[test]
    fn test_evaluate_and() {
        let schema = create_test_schema();
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef { table: None, column: "id".to_string() }),
                op: BinaryOperator::GreaterThan,
                right: Box::new(Expression::Literal(SqlValue::Integer(10))),
            }),
            op: BinaryOperator::And,
            right: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef { table: None, column: "id".to_string() }),
                op: BinaryOperator::LessThan,
                right: Box::new(Expression::Literal(SqlValue::Integer(100))),
            }),
        };

        let compiled = CompiledPredicate::compile(&expr, &schema);
        assert!(compiled.is_fully_compiled());

        // Test row that matches both conditions
        let row =
            Row::from_vec(vec![SqlValue::Integer(50), SqlValue::Varchar(arcstr::ArcStr::from("test"))]);
        assert_eq!(compiled.evaluate(&row), Some(true));

        // Test row that fails first condition
        let row = Row::from_vec(vec![SqlValue::Integer(5), SqlValue::Varchar(arcstr::ArcStr::from("test"))]);
        assert_eq!(compiled.evaluate(&row), Some(false));

        // Test row that fails second condition
        let row =
            Row::from_vec(vec![SqlValue::Integer(150), SqlValue::Varchar(arcstr::ArcStr::from("test"))]);
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
}
