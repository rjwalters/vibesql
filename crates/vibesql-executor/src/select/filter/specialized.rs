//! Specialized predicate evaluators for fast-path optimization
//!
//! These evaluators avoid SqlValue enum matching by working directly with
//! native types (f64, i64, Date), achieving 2-3x speedup on common predicates.

use vibesql_storage::Row;
use vibesql_types::SqlValue;

use super::pattern::{ComparisonOp, PredicatePattern};
use crate::errors::ExecutorError;

/// Trait for specialized predicate evaluation
pub(crate) trait PredicateEvaluator {
    /// Evaluate predicate for a single row
    /// Returns true if row matches predicate, false otherwise
    fn evaluate(&self, row: &Row) -> Result<bool, ExecutorError>;
}

/// Specialized evaluator for numeric comparisons (no enum matching)
#[derive(Debug)]
pub(crate) struct NumericComparisonEvaluator {
    col_idx: usize,
    op: ComparisonOp,
    constant: f64,
}

impl NumericComparisonEvaluator {
    pub(crate) fn new(col_idx: usize, op: ComparisonOp, constant: f64) -> Self {
        Self { col_idx, op, constant }
    }
}

impl PredicateEvaluator for NumericComparisonEvaluator {
    #[inline(always)]
    fn evaluate(&self, row: &Row) -> Result<bool, ExecutorError> {
        // Extract value from row
        let value = row
            .get(self.col_idx)
            .ok_or(ExecutorError::ColumnIndexOutOfBounds { index: self.col_idx })?;

        // NULL handling - NULL comparison returns false (SQL three-valued logic)
        // In WHERE clauses, NULL is treated as false
        if matches!(value, SqlValue::Null) {
            return Ok(false);
        }

        // Extract f64 value (fast path - minimal enum matching)
        let val_f64 = match value {
            SqlValue::Double(f) => *f,
            SqlValue::Float(f) => *f as f64,
            SqlValue::Real(f) => *f as f64,
            SqlValue::Numeric(f) => *f,
            SqlValue::Integer(i) => *i as f64,
            SqlValue::Smallint(i) => *i as f64,
            SqlValue::Bigint(i) => *i as f64,
            _ => {
                return Err(ExecutorError::TypeMismatch {
                    left: value.clone(),
                    op: format!("{:?}", self.op),
                    right: SqlValue::Double(self.constant),
                })
            }
        };

        // Perform comparison without enum matching
        let result = match self.op {
            ComparisonOp::Lt => val_f64 < self.constant,
            ComparisonOp::Le => val_f64 <= self.constant,
            ComparisonOp::Gt => val_f64 > self.constant,
            ComparisonOp::Ge => val_f64 >= self.constant,
            ComparisonOp::Eq => (val_f64 - self.constant).abs() < f64::EPSILON,
            ComparisonOp::NotEq => (val_f64 - self.constant).abs() >= f64::EPSILON,
        };

        Ok(result)
    }
}

/// Specialized evaluator for integer comparisons (no enum matching)
#[derive(Debug)]
pub(crate) struct IntegerComparisonEvaluator {
    col_idx: usize,
    op: ComparisonOp,
    constant: i64,
}

impl IntegerComparisonEvaluator {
    pub(crate) fn new(col_idx: usize, op: ComparisonOp, constant: i64) -> Self {
        Self { col_idx, op, constant }
    }
}

impl PredicateEvaluator for IntegerComparisonEvaluator {
    #[inline(always)]
    fn evaluate(&self, row: &Row) -> Result<bool, ExecutorError> {
        // Extract value from row
        let value = row
            .get(self.col_idx)
            .ok_or(ExecutorError::ColumnIndexOutOfBounds { index: self.col_idx })?;

        // NULL handling - NULL comparison returns false
        if matches!(value, SqlValue::Null) {
            return Ok(false);
        }

        // Try integer comparison first (fast path)
        if let Some(val_i64) = match value {
            SqlValue::Integer(i) => Some(*i),
            SqlValue::Smallint(i) => Some(*i as i64),
            SqlValue::Bigint(i) => Some(*i),
            _ => None,
        } {
            let result = match self.op {
                ComparisonOp::Lt => val_i64 < self.constant,
                ComparisonOp::Le => val_i64 <= self.constant,
                ComparisonOp::Gt => val_i64 > self.constant,
                ComparisonOp::Ge => val_i64 >= self.constant,
                ComparisonOp::Eq => val_i64 == self.constant,
                ComparisonOp::NotEq => val_i64 != self.constant,
            };
            return Ok(result);
        }

        // Fall back to float comparison for approximate numeric types
        // SQL standard requires implicit coercion between numeric types
        let val_f64 = match value {
            SqlValue::Double(f) => *f,
            SqlValue::Float(f) => *f as f64,
            SqlValue::Real(f) => *f as f64,
            SqlValue::Numeric(f) => *f,
            _ => {
                return Err(ExecutorError::TypeMismatch {
                    left: value.clone(),
                    op: format!("{:?}", self.op),
                    right: SqlValue::Bigint(self.constant),
                })
            }
        };

        let const_f64 = self.constant as f64;
        let result = match self.op {
            ComparisonOp::Lt => val_f64 < const_f64,
            ComparisonOp::Le => val_f64 <= const_f64,
            ComparisonOp::Gt => val_f64 > const_f64,
            ComparisonOp::Ge => val_f64 >= const_f64,
            ComparisonOp::Eq => (val_f64 - const_f64).abs() < f64::EPSILON,
            ComparisonOp::NotEq => (val_f64 - const_f64).abs() >= f64::EPSILON,
        };

        Ok(result)
    }
}

/// Specialized evaluator for date range checks (optimized for TPC-H)
#[derive(Debug)]
pub(crate) struct DateRangeEvaluator {
    col_idx: usize,
    min_date: vibesql_types::Date,
    max_date: vibesql_types::Date,
}

impl DateRangeEvaluator {
    pub(crate) fn new(
        col_idx: usize,
        min_date: vibesql_types::Date,
        max_date: vibesql_types::Date,
    ) -> Self {
        Self { col_idx, min_date, max_date }
    }
}

impl PredicateEvaluator for DateRangeEvaluator {
    #[inline(always)]
    fn evaluate(&self, row: &Row) -> Result<bool, ExecutorError> {
        // Extract value from row
        let value = row
            .get(self.col_idx)
            .ok_or(ExecutorError::ColumnIndexOutOfBounds { index: self.col_idx })?;

        // NULL handling - NULL comparison returns false
        if matches!(value, SqlValue::Null) {
            return Ok(false);
        }

        // Extract date value (fast path - single enum match)
        let date = match value {
            SqlValue::Date(d) => d,
            _ => {
                return Err(ExecutorError::TypeMismatch {
                    left: value.clone(),
                    op: "BETWEEN".to_string(),
                    right: SqlValue::Date(self.min_date),
                })
            }
        };

        // Perform range check: min <= date < max
        Ok(*date >= self.min_date && *date < self.max_date)
    }
}

/// Specialized evaluator for BETWEEN on numeric values
#[derive(Debug)]
pub(crate) struct BetweenNumericEvaluator {
    col_idx: usize,
    min: f64,
    max: f64,
}

impl BetweenNumericEvaluator {
    pub(crate) fn new(col_idx: usize, min: f64, max: f64) -> Self {
        Self { col_idx, min, max }
    }
}

impl PredicateEvaluator for BetweenNumericEvaluator {
    #[inline(always)]
    fn evaluate(&self, row: &Row) -> Result<bool, ExecutorError> {
        // Extract value from row
        let value = row
            .get(self.col_idx)
            .ok_or(ExecutorError::ColumnIndexOutOfBounds { index: self.col_idx })?;

        // NULL handling - NULL comparison returns false
        if matches!(value, SqlValue::Null) {
            return Ok(false);
        }

        // Extract f64 value (fast path - minimal enum matching)
        let val_f64 = match value {
            SqlValue::Double(f) => *f,
            SqlValue::Float(f) => *f as f64,
            SqlValue::Real(f) => *f as f64,
            SqlValue::Numeric(f) => *f,
            SqlValue::Integer(i) => *i as f64,
            SqlValue::Smallint(i) => *i as f64,
            SqlValue::Bigint(i) => *i as f64,
            _ => {
                return Err(ExecutorError::TypeMismatch {
                    left: value.clone(),
                    op: "BETWEEN".to_string(),
                    right: SqlValue::Double(self.min),
                })
            }
        };

        // Perform range check: min <= val <= max
        Ok(val_f64 >= self.min && val_f64 <= self.max)
    }
}

/// Create specialized evaluator from pattern
pub(crate) fn create_evaluator(pattern: &PredicatePattern) -> Option<Box<dyn PredicateEvaluator>> {
    match pattern {
        PredicatePattern::NumericComparison { col_idx, op, constant } => {
            Some(Box::new(NumericComparisonEvaluator::new(*col_idx, *op, *constant)))
        }
        PredicatePattern::IntegerComparison { col_idx, op, constant } => {
            Some(Box::new(IntegerComparisonEvaluator::new(*col_idx, *op, *constant)))
        }
        PredicatePattern::DateRange { col_idx, min_date, max_date } => {
            Some(Box::new(DateRangeEvaluator::new(*col_idx, *min_date, *max_date)))
        }
        PredicatePattern::BetweenNumeric { col_idx, min, max } => {
            Some(Box::new(BetweenNumericEvaluator::new(*col_idx, *min, *max)))
        }
        PredicatePattern::General(_) => None, // No specialized evaluator
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_row(values: Vec<SqlValue>) -> Row {
        Row::from_vec(values)
    }

    #[test]
    fn test_numeric_comparison_less_than() {
        let evaluator = NumericComparisonEvaluator::new(0, ComparisonOp::Lt, 100.0);

        // Test: 50.0 < 100.0 = true
        let row = create_test_row(vec![SqlValue::Double(50.0)]);
        assert!(evaluator.evaluate(&row).unwrap());

        // Test: 150.0 < 100.0 = false
        let row = create_test_row(vec![SqlValue::Double(150.0)]);
        assert!(!evaluator.evaluate(&row).unwrap());

        // Test: NULL < 100.0 = false
        let row = create_test_row(vec![SqlValue::Null]);
        assert!(!evaluator.evaluate(&row).unwrap());
    }

    #[test]
    fn test_integer_comparison_greater_than() {
        let evaluator = IntegerComparisonEvaluator::new(0, ComparisonOp::Gt, 10);

        // Test: 20 > 10 = true
        let row = create_test_row(vec![SqlValue::Integer(20)]);
        assert!(evaluator.evaluate(&row).unwrap());

        // Test: 5 > 10 = false
        let row = create_test_row(vec![SqlValue::Integer(5)]);
        assert!(!evaluator.evaluate(&row).unwrap());

        // Test: NULL > 10 = false
        let row = create_test_row(vec![SqlValue::Null]);
        assert!(!evaluator.evaluate(&row).unwrap());
    }

    #[test]
    fn test_integer_comparison_with_float_column() {
        // Issue #2609: IntegerComparisonEvaluator must handle Float column values
        // This happens when comparing a Float column with an Integer literal

        // Test: Float(822.1) >= -10 = true
        let evaluator = IntegerComparisonEvaluator::new(0, ComparisonOp::Ge, -10);
        let row = create_test_row(vec![SqlValue::Float(822.1)]);
        assert!(evaluator.evaluate(&row).unwrap());

        // Test: Float(593.78) >= -77 = true
        let evaluator = IntegerComparisonEvaluator::new(0, ComparisonOp::Ge, -77);
        let row = create_test_row(vec![SqlValue::Float(593.78)]);
        assert!(evaluator.evaluate(&row).unwrap());

        // Test: Float(-100.5) >= -10 = false
        let evaluator = IntegerComparisonEvaluator::new(0, ComparisonOp::Ge, -10);
        let row = create_test_row(vec![SqlValue::Float(-100.5)]);
        assert!(!evaluator.evaluate(&row).unwrap());

        // Test: Double column with integer constant
        let evaluator = IntegerComparisonEvaluator::new(0, ComparisonOp::Lt, 100);
        let row = create_test_row(vec![SqlValue::Double(50.5)]);
        assert!(evaluator.evaluate(&row).unwrap());

        // Test: Real column with integer constant
        let evaluator = IntegerComparisonEvaluator::new(0, ComparisonOp::Gt, 0);
        let row = create_test_row(vec![SqlValue::Real(25.0)]);
        assert!(evaluator.evaluate(&row).unwrap());

        // Test: Numeric column with integer constant
        let evaluator = IntegerComparisonEvaluator::new(0, ComparisonOp::Le, 50);
        let row = create_test_row(vec![SqlValue::Numeric(49.99)]);
        assert!(evaluator.evaluate(&row).unwrap());
    }

    #[test]
    fn test_date_range() {
        let min_date: vibesql_types::Date = "1994-01-01".parse().unwrap();
        let max_date: vibesql_types::Date = "1995-01-01".parse().unwrap();
        let evaluator = DateRangeEvaluator::new(0, min_date, max_date);

        // Test: '1994-06-01' in range = true
        let test_date: vibesql_types::Date = "1994-06-01".parse().unwrap();
        let row = create_test_row(vec![SqlValue::Date(test_date)]);
        assert!(evaluator.evaluate(&row).unwrap());

        // Test: '1993-12-31' before range = false
        let test_date: vibesql_types::Date = "1993-12-31".parse().unwrap();
        let row = create_test_row(vec![SqlValue::Date(test_date)]);
        assert!(!evaluator.evaluate(&row).unwrap());

        // Test: '1995-01-01' at upper bound (exclusive) = false
        let test_date: vibesql_types::Date = "1995-01-01".parse().unwrap();
        let row = create_test_row(vec![SqlValue::Date(test_date)]);
        assert!(!evaluator.evaluate(&row).unwrap());

        // Test: NULL = false
        let row = create_test_row(vec![SqlValue::Null]);
        assert!(!evaluator.evaluate(&row).unwrap());
    }

    #[test]
    fn test_between_numeric() {
        let evaluator = BetweenNumericEvaluator::new(0, 10.0, 100.0);

        // Test: 50.0 in range = true
        let row = create_test_row(vec![SqlValue::Double(50.0)]);
        assert!(evaluator.evaluate(&row).unwrap());

        // Test: 10.0 at lower bound (inclusive) = true
        let row = create_test_row(vec![SqlValue::Double(10.0)]);
        assert!(evaluator.evaluate(&row).unwrap());

        // Test: 100.0 at upper bound (inclusive) = true
        let row = create_test_row(vec![SqlValue::Double(100.0)]);
        assert!(evaluator.evaluate(&row).unwrap());

        // Test: 5.0 below range = false
        let row = create_test_row(vec![SqlValue::Double(5.0)]);
        assert!(!evaluator.evaluate(&row).unwrap());

        // Test: 150.0 above range = false
        let row = create_test_row(vec![SqlValue::Double(150.0)]);
        assert!(!evaluator.evaluate(&row).unwrap());

        // Test: NULL = false
        let row = create_test_row(vec![SqlValue::Null]);
        assert!(!evaluator.evaluate(&row).unwrap());
    }
}
