//! Compiled predicate evaluation for fast-path WHERE clauses
//!
//! This module provides specialized evaluation for simple predicates commonly found
//! in analytical queries (e.g., TPC-H Q6). Instead of walking the full expression tree
//! for each row, we "compile" predicates into a more efficient representation.
//!
//! Optimizations:
//! - Pre-compute column indices to avoid schema lookups
//! - Flatten AND chains into a vector of predicates
//! - Use type-specific comparison functions to skip type coercion

#![allow(clippy::redundant_guards)]
//! - Direct memory access to column values

use crate::{errors::ExecutorError, schema::CombinedSchema};
use vibesql_ast::{BinaryOperator, Expression};
use vibesql_storage::Row;
use vibesql_types::SqlValue;

/// A compiled simple predicate for fast evaluation
#[derive(Debug, Clone)]
enum CompiledPredicate {
    /// Column comparison: column_idx op literal_value
    ColumnLiteral { column_idx: usize, op: ComparisonOp, literal: SqlValue },
    /// BETWEEN: column_idx BETWEEN low AND high
    Between { column_idx: usize, low: SqlValue, high: SqlValue, negated: bool, symmetric: bool },
}

impl CompiledPredicate {
    /// Estimate the selectivity of this predicate (fraction of rows that pass)
    /// Lower selectivity = more selective (fewer rows pass) = evaluate first
    fn estimate_selectivity(&self) -> f64 {
        match self {
            // Equality predicates are very selective (~0.1% of rows match)
            CompiledPredicate::ColumnLiteral { op: ComparisonOp::Equal, .. } => 0.001,

            // Inequality is very unselective (~99% of rows match)
            CompiledPredicate::ColumnLiteral { op: ComparisonOp::NotEqual, .. } => 0.99,

            // Range predicates: estimate based on operator
            // < and > assume ~33% selectivity (rough estimate)
            CompiledPredicate::ColumnLiteral { op: ComparisonOp::LessThan, .. } => 0.33,
            CompiledPredicate::ColumnLiteral { op: ComparisonOp::GreaterThan, .. } => 0.33,

            // <= and >= assume ~40% selectivity (slightly less selective)
            CompiledPredicate::ColumnLiteral { op: ComparisonOp::LessThanOrEqual, .. } => 0.40,
            CompiledPredicate::ColumnLiteral { op: ComparisonOp::GreaterThanOrEqual, .. } => 0.40,

            // BETWEEN is moderately selective (~10% of rows)
            CompiledPredicate::Between { negated: false, .. } => 0.10,

            // NOT BETWEEN is unselective (~90% of rows)
            CompiledPredicate::Between { negated: true, .. } => 0.90,
        }
    }

    /// Get the column index referenced by this predicate
    fn column_idx(&self) -> usize {
        match self {
            CompiledPredicate::ColumnLiteral { column_idx, .. } => *column_idx,
            CompiledPredicate::Between { column_idx, .. } => *column_idx,
        }
    }

    /// Check if this predicate is IS NULL check
    fn is_null_check(&self) -> bool {
        matches!(
            self,
            CompiledPredicate::ColumnLiteral {
                op: ComparisonOp::Equal,
                literal: SqlValue::Null,
                ..
            }
        )
    }

    /// Check if this predicate requires non-null value (any comparison except IS NULL)
    fn requires_non_null(&self) -> bool {
        match self {
            CompiledPredicate::ColumnLiteral { literal, .. } => !matches!(literal, SqlValue::Null),
            CompiledPredicate::Between { .. } => true,
        }
    }
}

/// Supported comparison operators
#[derive(Debug, Clone, Copy)]
enum ComparisonOp {
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
}

/// A compiled WHERE clause consisting of AND-combined simple predicates
pub struct CompiledWhereClause {
    predicates: Vec<CompiledPredicate>,
}

impl CompiledWhereClause {
    /// Try to compile a WHERE clause into the optimized form
    /// Returns None if the WHERE clause contains unsupported patterns
    pub fn try_compile(where_expr: &Expression, schema: &CombinedSchema) -> Option<Self> {
        let mut predicates = Vec::new();

        // Try to extract AND-combined predicates
        if !Self::extract_and_predicates(where_expr, schema, &mut predicates) {
            return None;
        }

        if predicates.is_empty() {
            return None;
        }

        // Detect contradictions - if found, the WHERE clause is always false
        if Self::detect_contradictions(&predicates) {
            // Return a special "always false" compiled clause with impossible predicate
            // This will cause the scan to return 0 rows efficiently
            return None; // For now, fall back to regular evaluation (correctness first)
        }

        // Reorder predicates by selectivity (most selective first)
        // This improves performance by failing early on rows that don't match
        Self::reorder_by_selectivity(&mut predicates);

        Some(CompiledWhereClause { predicates })
    }

    /// Detect contradictions in predicates that make the WHERE clause always false
    fn detect_contradictions(predicates: &[CompiledPredicate]) -> bool {
        // Group predicates by column
        use std::collections::HashMap;
        let mut by_column: HashMap<usize, Vec<&CompiledPredicate>> = HashMap::new();

        for pred in predicates {
            by_column.entry(pred.column_idx()).or_default().push(pred);
        }

        // Check each column's predicates for contradictions
        for (_col_idx, col_preds) in by_column.iter() {
            // Check for "IS NULL AND <comparison>" contradiction
            let has_null_check = col_preds.iter().any(|p| p.is_null_check());
            let has_non_null_req = col_preds.iter().any(|p| p.requires_non_null());

            if has_null_check && has_non_null_req {
                // Contradiction: col IS NULL AND col > X (impossible)
                return true;
            }

            // TODO: More sophisticated contradiction detection:
            // - Multiple equality checks: col = 1 AND col = 2
            // - Range contradictions: col > 100 AND col < 50
            // - BETWEEN contradictions: col BETWEEN 1 AND 10 AND col > 100
        }

        false
    }

    /// Reorder predicates by selectivity (most selective first)
    /// This improves performance by evaluating cheap, selective predicates early
    fn reorder_by_selectivity(predicates: &mut [CompiledPredicate]) {
        // Sort by selectivity (ascending = most selective first)
        predicates.sort_by(|a, b| {
            a.estimate_selectivity()
                .partial_cmp(&b.estimate_selectivity())
                .unwrap_or(std::cmp::Ordering::Equal)
        });
    }

    /// Extract predicates from AND-combined expression
    fn extract_and_predicates(
        expr: &Expression,
        schema: &CombinedSchema,
        predicates: &mut Vec<CompiledPredicate>,
    ) -> bool {
        match expr {
            Expression::BinaryOp { left, op: BinaryOperator::And, right } => {
                // Recursively extract from left and right
                Self::extract_and_predicates(left, schema, predicates)
                    && Self::extract_and_predicates(right, schema, predicates)
            }
            Expression::Between { expr: col_expr, low, high, negated, symmetric } => {
                // Try to compile BETWEEN predicate
                Self::try_compile_between(
                    col_expr, low, high, *negated, *symmetric, schema, predicates,
                )
            }
            _ => {
                // Try to compile as simple binary comparison
                Self::try_compile_comparison(expr, schema, predicates)
            }
        }
    }

    /// Try to compile a BETWEEN predicate
    fn try_compile_between(
        col_expr: &Expression,
        low_expr: &Expression,
        high_expr: &Expression,
        negated: bool,
        symmetric: bool,
        schema: &CombinedSchema,
        predicates: &mut Vec<CompiledPredicate>,
    ) -> bool {
        // Extract column reference
        let column_idx = match col_expr {
            Expression::ColumnRef { table, column } => {
                schema.get_column_index(table.as_deref(), column)
            }
            _ => None,
        };

        let column_idx = match column_idx {
            Some(idx) => idx,
            None => return false,
        };

        // Extract literal values
        let low = match low_expr {
            Expression::Literal(val) => val.clone(),
            _ => return false,
        };

        let high = match high_expr {
            Expression::Literal(val) => val.clone(),
            _ => return false,
        };

        // Reject NULL bounds - fall back to regular evaluator for proper 3-valued logic
        // Issue #2633: NOT BETWEEN with NULL bound requires NULL result, not boolean negation
        if matches!(low, SqlValue::Null) || matches!(high, SqlValue::Null) {
            return false;
        }

        predicates.push(CompiledPredicate::Between { column_idx, low, high, negated, symmetric });

        true
    }

    /// Try to compile a simple comparison predicate
    fn try_compile_comparison(
        expr: &Expression,
        schema: &CombinedSchema,
        predicates: &mut Vec<CompiledPredicate>,
    ) -> bool {
        // Must be a binary operation
        let (left, op, right) = match expr {
            Expression::BinaryOp { left, op, right } => (left, op, right),
            _ => return false,
        };

        // Convert operator
        let comp_op = match op {
            BinaryOperator::Equal => ComparisonOp::Equal,
            BinaryOperator::NotEqual => ComparisonOp::NotEqual,
            BinaryOperator::LessThan => ComparisonOp::LessThan,
            BinaryOperator::LessThanOrEqual => ComparisonOp::LessThanOrEqual,
            BinaryOperator::GreaterThan => ComparisonOp::GreaterThan,
            BinaryOperator::GreaterThanOrEqual => ComparisonOp::GreaterThanOrEqual,
            _ => return false, // Not a comparison operator
        };

        // Try column on left, literal on right
        if let (Some(column_idx), Some(literal)) =
            (Self::try_extract_column(left, schema), Self::try_extract_literal(right))
        {
            predicates.push(CompiledPredicate::ColumnLiteral { column_idx, op: comp_op, literal });
            return true;
        }

        // Try literal on left, column on right (flip operator)
        if let (Some(literal), Some(column_idx)) =
            (Self::try_extract_literal(left), Self::try_extract_column(right, schema))
        {
            let flipped_op = Self::flip_operator(comp_op);
            predicates.push(CompiledPredicate::ColumnLiteral {
                column_idx,
                op: flipped_op,
                literal,
            });
            return true;
        }

        false
    }

    /// Try to extract a column index from an expression
    fn try_extract_column(expr: &Expression, schema: &CombinedSchema) -> Option<usize> {
        match expr {
            Expression::ColumnRef { table, column } => {
                schema.get_column_index(table.as_deref(), column)
            }
            _ => None,
        }
    }

    /// Try to extract a literal value from an expression
    fn try_extract_literal(expr: &Expression) -> Option<SqlValue> {
        match expr {
            Expression::Literal(val) => Some(val.clone()),
            _ => None,
        }
    }

    /// Flip a comparison operator for when operands are swapped
    fn flip_operator(op: ComparisonOp) -> ComparisonOp {
        match op {
            ComparisonOp::Equal => ComparisonOp::Equal,
            ComparisonOp::NotEqual => ComparisonOp::NotEqual,
            ComparisonOp::LessThan => ComparisonOp::GreaterThan,
            ComparisonOp::LessThanOrEqual => ComparisonOp::GreaterThanOrEqual,
            ComparisonOp::GreaterThan => ComparisonOp::LessThan,
            ComparisonOp::GreaterThanOrEqual => ComparisonOp::LessThanOrEqual,
        }
    }

    /// Evaluate the compiled predicates on a row (returns true if row matches ALL predicates)
    #[inline(always)]
    pub fn evaluate(&self, row: &Row) -> Result<bool, ExecutorError> {
        for predicate in &self.predicates {
            if !self.evaluate_single(predicate, row)? {
                return Ok(false); // Short-circuit: AND semantics
            }
        }
        Ok(true)
    }

    /// Evaluate a single compiled predicate
    #[inline(always)]
    fn evaluate_single(
        &self,
        predicate: &CompiledPredicate,
        row: &Row,
    ) -> Result<bool, ExecutorError> {
        match predicate {
            CompiledPredicate::ColumnLiteral { column_idx, op, literal } => {
                let column_value = &row.values[*column_idx];
                self.compare_values(column_value, literal, *op)
            }
            CompiledPredicate::Between { column_idx, low, high, negated, symmetric } => {
                let column_value = &row.values[*column_idx];
                let result = self.is_between(column_value, low, high, *symmetric)?;
                Ok(if *negated { !result } else { result })
            }
        }
    }

    /// Fast comparison of SQL values with type-specific logic
    #[inline(always)]
    fn compare_values(
        &self,
        left: &SqlValue,
        right: &SqlValue,
        op: ComparisonOp,
    ) -> Result<bool, ExecutorError> {
        use SqlValue::*;

        // Handle NULL - comparisons with NULL return NULL (false in WHERE context)
        if matches!(left, Null) || matches!(right, Null) {
            return Ok(false);
        }

        // Fast path for common types - avoid generic comparison overhead
        match (left, right) {
            // Integer comparisons
            (Integer(l), Integer(r)) => Ok(self.apply_op(*l, *r, op)),
            (Bigint(l), Bigint(r)) => Ok(self.apply_op(*l, *r, op)),
            (Smallint(l), Smallint(r)) => Ok(self.apply_op(*l, *r, op)),

            // Float comparisons - use PartialOrd since floats don't impl Ord
            (Double(l), Double(r)) => Ok(self.apply_op_float(*l, *r, op)),
            (Float(l), Float(r)) => Ok(self.apply_op_float(*l, *r, op)),
            (Real(l), Real(r)) => Ok(self.apply_op_float(*l, *r, op)),

            // String comparisons (dates are stored as strings in TPC-H)
            (Varchar(l), Varchar(r))
            | (Character(l), Character(r))
            | (Varchar(l), Character(r))
            | (Character(l), Varchar(r)) => Ok(self.apply_op(&**l, &**r, op)),

            // Cross-type comparisons - fall back to slower path
            _ => self.compare_values_generic(left, right, op),
        }
    }

    /// Generic comparison for cross-type cases
    fn compare_values_generic(
        &self,
        left: &SqlValue,
        right: &SqlValue,
        op: ComparisonOp,
    ) -> Result<bool, ExecutorError> {
        // Use the existing operator registry for correctness
        use crate::evaluator::operators::OperatorRegistry;
        use vibesql_ast::BinaryOperator;

        let ast_op = match op {
            ComparisonOp::Equal => BinaryOperator::Equal,
            ComparisonOp::NotEqual => BinaryOperator::NotEqual,
            ComparisonOp::LessThan => BinaryOperator::LessThan,
            ComparisonOp::LessThanOrEqual => BinaryOperator::LessThanOrEqual,
            ComparisonOp::GreaterThan => BinaryOperator::GreaterThan,
            ComparisonOp::GreaterThanOrEqual => BinaryOperator::GreaterThanOrEqual,
        };

        let result = OperatorRegistry::eval_binary_op(left, &ast_op, right, Default::default())?;

        match result {
            SqlValue::Boolean(b) => Ok(b),
            SqlValue::Null => Ok(false),
            _ => {
                Err(ExecutorError::InvalidWhereClause("Comparison must return boolean".to_string()))
            }
        }
    }

    /// Apply comparison operator to Ord types
    #[inline(always)]
    fn apply_op<T: Ord>(&self, left: T, right: T, op: ComparisonOp) -> bool {
        match op {
            ComparisonOp::Equal => left == right,
            ComparisonOp::NotEqual => left != right,
            ComparisonOp::LessThan => left < right,
            ComparisonOp::LessThanOrEqual => left <= right,
            ComparisonOp::GreaterThan => left > right,
            ComparisonOp::GreaterThanOrEqual => left >= right,
        }
    }

    /// Apply comparison operator to float types (PartialOrd)
    #[inline(always)]
    fn apply_op_float<T: PartialOrd>(&self, left: T, right: T, op: ComparisonOp) -> bool {
        match op {
            ComparisonOp::Equal => left == right,
            ComparisonOp::NotEqual => left != right,
            ComparisonOp::LessThan => left < right,
            ComparisonOp::LessThanOrEqual => left <= right,
            ComparisonOp::GreaterThan => left > right,
            ComparisonOp::GreaterThanOrEqual => left >= right,
        }
    }

    /// Check if value is between low and high (inclusive)
    /// For SYMMETRIC: swap bounds if low > high before comparison
    #[inline(always)]
    fn is_between(
        &self,
        value: &SqlValue,
        low: &SqlValue,
        high: &SqlValue,
        symmetric: bool,
    ) -> Result<bool, ExecutorError> {
        // Handle SYMMETRIC: swap bounds if low > high
        let (effective_low, effective_high) = if symmetric {
            // Check if bounds need swapping
            let low_gt_high = self.compare_values(low, high, ComparisonOp::GreaterThan)?;
            if low_gt_high {
                (high, low)
            } else {
                (low, high)
            }
        } else {
            (low, high)
        };

        // BETWEEN is inclusive on both ends: value >= low AND value <= high
        let ge_low = self.compare_values(value, effective_low, ComparisonOp::GreaterThanOrEqual)?;
        let le_high = self.compare_values(value, effective_high, ComparisonOp::LessThanOrEqual)?;
        Ok(ge_low && le_high)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::DataType;

    fn make_test_schema() -> CombinedSchema {
        let schema = TableSchema::new(
            "lineitem".to_string(),
            vec![
                ColumnSchema {
                    name: "l_quantity".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    default_value: None,
                },
                ColumnSchema {
                    name: "l_discount".to_string(),
                    data_type: DataType::DoublePrecision,
                    nullable: false,
                    default_value: None,
                },
                ColumnSchema {
                    name: "l_shipdate".to_string(),
                    data_type: DataType::Varchar { max_length: None },
                    nullable: false,
                    default_value: None,
                },
            ],
        );
        CombinedSchema::from_table("lineitem".to_string(), schema)
    }

    #[test]
    fn test_compile_simple_comparison() {
        let schema = make_test_schema();

        // l_quantity < 24
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "l_quantity".to_string() }),
            op: BinaryOperator::LessThan,
            right: Box::new(Expression::Literal(SqlValue::Integer(24))),
        };

        let compiled = CompiledWhereClause::try_compile(&expr, &schema);
        assert!(compiled.is_some());

        let compiled = compiled.unwrap();
        assert_eq!(compiled.predicates.len(), 1);
    }

    #[test]
    fn test_compile_and_chain() {
        let schema = make_test_schema();

        // l_quantity < 24 AND l_discount >= 0.05
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "l_quantity".to_string(),
                }),
                op: BinaryOperator::LessThan,
                right: Box::new(Expression::Literal(SqlValue::Integer(24))),
            }),
            op: BinaryOperator::And,
            right: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "l_discount".to_string(),
                }),
                op: BinaryOperator::GreaterThanOrEqual,
                right: Box::new(Expression::Literal(SqlValue::Double(0.05))),
            }),
        };

        let compiled = CompiledWhereClause::try_compile(&expr, &schema);
        assert!(compiled.is_some());

        let compiled = compiled.unwrap();
        assert_eq!(compiled.predicates.len(), 2);
    }

    #[test]
    fn test_evaluate_predicate() {
        let schema = make_test_schema();

        // l_quantity < 24
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "l_quantity".to_string() }),
            op: BinaryOperator::LessThan,
            right: Box::new(Expression::Literal(SqlValue::Integer(24))),
        };

        let compiled = CompiledWhereClause::try_compile(&expr, &schema).unwrap();

        // Test row that matches
        let row = Row::from_vec(vec![
                SqlValue::Integer(20),
                SqlValue::Double(0.05),
                SqlValue::Varchar(arcstr::ArcStr::from("1994-01-01")),
            ]);
        assert!(compiled.evaluate(&row).unwrap());

        // Test row that doesn't match
        let row2 = Row::from_vec(vec![
                SqlValue::Integer(30),
                SqlValue::Double(0.05),
                SqlValue::Varchar(arcstr::ArcStr::from("1994-01-01")),
            ]);
        assert!(!compiled.evaluate(&row2).unwrap());
    }

    #[test]
    fn test_predicate_reordering_by_selectivity() {
        let schema = make_test_schema();

        // Build: l_quantity < 100 AND l_discount = 0.05
        // Equality (l_discount = 0.05) should be evaluated first (more selective)
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "l_quantity".to_string(),
                }),
                op: BinaryOperator::LessThan,
                right: Box::new(Expression::Literal(SqlValue::Integer(100))),
            }),
            op: BinaryOperator::And,
            right: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "l_discount".to_string(),
                }),
                op: BinaryOperator::Equal,
                right: Box::new(Expression::Literal(SqlValue::Double(0.05))),
            }),
        };

        let compiled = CompiledWhereClause::try_compile(&expr, &schema).unwrap();

        // First predicate should be the equality (more selective)
        // We can't directly access predicates, but we can verify compilation succeeded
        assert_eq!(compiled.predicates.len(), 2);

        // Verify that equality predicate comes first (most selective)
        match &compiled.predicates[0] {
            CompiledPredicate::ColumnLiteral { op: ComparisonOp::Equal, .. } => {
                // Good! Equality is first
            }
            _ => panic!("Expected equality predicate to be first due to reordering"),
        }
    }

    #[test]
    fn test_selectivity_estimation() {
        // Verify selectivity estimates are in expected ranges
        let equal_pred = CompiledPredicate::ColumnLiteral {
            column_idx: 0,
            op: ComparisonOp::Equal,
            literal: SqlValue::Integer(42),
        };
        assert_eq!(equal_pred.estimate_selectivity(), 0.001);

        let not_equal_pred = CompiledPredicate::ColumnLiteral {
            column_idx: 0,
            op: ComparisonOp::NotEqual,
            literal: SqlValue::Integer(42),
        };
        assert_eq!(not_equal_pred.estimate_selectivity(), 0.99);

        let between_pred = CompiledPredicate::Between {
            column_idx: 0,
            low: SqlValue::Integer(1),
            high: SqlValue::Integer(10),
            negated: false,
            symmetric: false,
        };
        assert_eq!(between_pred.estimate_selectivity(), 0.10);
    }

    #[test]
    fn test_short_circuit_evaluation() {
        let schema = make_test_schema();

        // l_discount = 999.99 AND l_quantity < 24
        // First predicate will fail, second should not be evaluated (but we can't verify this directly)
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "l_discount".to_string(),
                }),
                op: BinaryOperator::Equal,
                right: Box::new(Expression::Literal(SqlValue::Double(999.99))),
            }),
            op: BinaryOperator::And,
            right: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "l_quantity".to_string(),
                }),
                op: BinaryOperator::LessThan,
                right: Box::new(Expression::Literal(SqlValue::Integer(24))),
            }),
        };

        let compiled = CompiledWhereClause::try_compile(&expr, &schema).unwrap();

        // Test row where first predicate fails
        let row = Row::from_vec(vec![
                SqlValue::Integer(10),  // l_quantity
                SqlValue::Double(0.05), // l_discount (doesn't match 999.99)
                SqlValue::Varchar(arcstr::ArcStr::from("1994-01-01")),
            ]);

        // Should return false (short-circuit on first predicate)
        assert!(!compiled.evaluate(&row).unwrap());
    }

    #[test]
    fn test_null_semantics_preserved() {
        let schema = make_test_schema();

        // l_quantity > 10
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "l_quantity".to_string() }),
            op: BinaryOperator::GreaterThan,
            right: Box::new(Expression::Literal(SqlValue::Integer(10))),
        };

        let compiled = CompiledWhereClause::try_compile(&expr, &schema).unwrap();

        // Test row with NULL value
        let row = Row::from_vec(vec![
                SqlValue::Null, // l_quantity is NULL
                SqlValue::Double(0.05),
                SqlValue::Varchar(arcstr::ArcStr::from("1994-01-01")),
            ]);

        // NULL > 10 should return false (not true, not NULL)
        assert!(!compiled.evaluate(&row).unwrap());
    }

    #[test]
    fn test_between_with_null_bound_not_compiled() {
        // Issue #2633: BETWEEN with NULL bounds should NOT be compiled
        // to avoid incorrect 3-valued logic handling in the fast path
        let schema = make_test_schema();

        // l_quantity NOT BETWEEN 31 AND NULL
        // This should fall back to regular evaluator for proper NULL semantics
        let expr = Expression::Between {
            expr: Box::new(Expression::ColumnRef { table: None, column: "l_quantity".to_string() }),
            low: Box::new(Expression::Literal(SqlValue::Integer(31))),
            high: Box::new(Expression::Literal(SqlValue::Null)),
            negated: true,
            symmetric: false,
        };

        // Should NOT compile - returns None to fall back to regular evaluator
        let compiled = CompiledWhereClause::try_compile(&expr, &schema);
        assert!(compiled.is_none(), "BETWEEN with NULL bound should not be compiled");

        // Also test with NULL low bound
        let expr_null_low = Expression::Between {
            expr: Box::new(Expression::ColumnRef { table: None, column: "l_quantity".to_string() }),
            low: Box::new(Expression::Literal(SqlValue::Null)),
            high: Box::new(Expression::Literal(SqlValue::Integer(100))),
            negated: false,
            symmetric: false,
        };

        let compiled_null_low = CompiledWhereClause::try_compile(&expr_null_low, &schema);
        assert!(compiled_null_low.is_none(), "BETWEEN with NULL low bound should not be compiled");
    }

    #[test]
    fn test_between_with_non_null_bounds_compiled() {
        // Verify that BETWEEN with non-NULL bounds still compiles correctly
        let schema = make_test_schema();

        // l_quantity BETWEEN 10 AND 100
        let expr = Expression::Between {
            expr: Box::new(Expression::ColumnRef { table: None, column: "l_quantity".to_string() }),
            low: Box::new(Expression::Literal(SqlValue::Integer(10))),
            high: Box::new(Expression::Literal(SqlValue::Integer(100))),
            negated: false,
            symmetric: false,
        };

        let compiled = CompiledWhereClause::try_compile(&expr, &schema);
        assert!(compiled.is_some(), "BETWEEN with non-NULL bounds should compile");

        let compiled = compiled.unwrap();

        // Test row in range
        let row_in_range = Row::from_vec(vec![
                SqlValue::Integer(50),
                SqlValue::Double(0.05),
                SqlValue::Varchar(arcstr::ArcStr::from("1994-01-01")),
            ]);
        assert!(compiled.evaluate(&row_in_range).unwrap());

        // Test row out of range
        let row_out_of_range = Row::from_vec(vec![
                SqlValue::Integer(200),
                SqlValue::Double(0.05),
                SqlValue::Varchar(arcstr::ArcStr::from("1994-01-01")),
            ]);
        assert!(!compiled.evaluate(&row_out_of_range).unwrap());
    }
}
