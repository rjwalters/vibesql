//! NULL value detection for SIMD expression evaluation
//!
//! This module handles NULL value detection to enable graceful fallback
//! from SIMD to scalar evaluation when NULL values are present.
//!
//! # Why Graceful Fallback?
//!
//! SIMD operations cannot handle NULL values efficiently because:
//! - NULLs require per-element branching which defeats SIMD parallelism
//! - SQL NULL semantics (NULL propagation) are complex
//! - Mixing NULLs with valid values in SIMD lanes is error-prone
//!
//! Instead, we detect NULLs early and fall back to scalar evaluation,
//! which correctly handles SQL NULL semantics.

use vibesql_ast::Expression;
use vibesql_types::SqlValue;

use super::analysis::extract_column_refs;

/// Check if an expression contains NULL values by checking source columns
///
/// This function samples the input rows to detect NULL values that would
/// cause issues in SIMD evaluation. If NULLs are found, the caller should
/// fall back to scalar evaluation.
///
/// # Algorithm
///
/// 1. Extract all column references from the expression
/// 2. Sample rows to check for NULL values in those columns
/// 3. Return true if any NULL is found
///
/// # Performance Note
///
/// For large datasets, this samples the first 100 rows, then continues
/// checking remaining rows. This balances early detection with thoroughness.
#[cfg(feature = "simd")]
pub fn has_null_values(
    expr: &Expression,
    rows: &[vibesql_storage::Row],
    evaluator: &crate::evaluator::CombinedExpressionEvaluator,
) -> bool {
    let column_refs = extract_column_refs(expr);
    if column_refs.is_empty() {
        return false;
    }

    let sample_size = rows.len().min(100);

    for row in rows.iter().take(sample_size) {
        for col_ref in &column_refs {
            match evaluator.eval(col_ref, row) {
                Ok(value) if value == SqlValue::Null => return true,
                Err(_) => return true,
                _ => {}
            }
        }
    }

    if sample_size < rows.len() {
        for row in rows.iter().skip(sample_size) {
            for col_ref in &column_refs {
                match evaluator.eval(col_ref, row) {
                    Ok(value) if value == SqlValue::Null => return true,
                    Err(_) => return true,
                    _ => {}
                }
            }
        }
    }

    false
}

#[cfg(all(test, feature = "simd"))]
mod tests {
    use super::*;
    use vibesql_ast::BinaryOperator;
    use vibesql_storage::Row;
    use vibesql_types::{DataType, SqlValue};

    // Helper to create a mock evaluator for testing
    fn create_test_evaluator() -> crate::evaluator::CombinedExpressionEvaluator<'static> {
        use crate::schema::CombinedSchema;
        use vibesql_catalog::{ColumnSchema, TableSchema};

        let columns = vec![
            ColumnSchema::new("a".to_string(), DataType::Bigint, true), // nullable
            ColumnSchema::new("b".to_string(), DataType::Bigint, false),
        ];
        let table_schema = TableSchema::new("test".to_string(), columns);

        let schema = Box::leak(Box::new(CombinedSchema::from_table(
            "test".to_string(),
            table_schema,
        )));
        crate::evaluator::CombinedExpressionEvaluator::new(schema)
    }

    #[test]
    fn test_has_null_values_returns_false_for_non_null_data() {
        let evaluator = create_test_evaluator();
        let rows: Vec<Row> = (0..100)
            .map(|i| Row::new(vec![SqlValue::Bigint(i), SqlValue::Bigint(i * 2)]))
            .collect();

        let expr = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef {
                table: None,
                column: "a".to_string(),
            }),
            op: BinaryOperator::Plus,
            right: Box::new(Expression::ColumnRef {
                table: None,
                column: "b".to_string(),
            }),
        };

        assert!(!has_null_values(&expr, &rows, &evaluator));
    }

    #[test]
    fn test_has_null_values_returns_true_when_null_present() {
        let evaluator = create_test_evaluator();
        let mut rows: Vec<Row> = (0..99)
            .map(|i| Row::new(vec![SqlValue::Bigint(i), SqlValue::Bigint(i * 2)]))
            .collect();
        // Add a row with NULL
        rows.push(Row::new(vec![SqlValue::Null, SqlValue::Bigint(100)]));

        let expr = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef {
                table: None,
                column: "a".to_string(),
            }),
            op: BinaryOperator::Plus,
            right: Box::new(Expression::ColumnRef {
                table: None,
                column: "b".to_string(),
            }),
        };

        assert!(has_null_values(&expr, &rows, &evaluator));
    }

    #[test]
    fn test_has_null_values_returns_false_for_literals_only() {
        let evaluator = create_test_evaluator();
        let rows: Vec<Row> = (0..100)
            .map(|i| Row::new(vec![SqlValue::Bigint(i), SqlValue::Bigint(i * 2)]))
            .collect();

        // Expression with only literals, no column refs
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::Literal(SqlValue::Integer(10))),
            op: BinaryOperator::Plus,
            right: Box::new(Expression::Literal(SqlValue::Integer(20))),
        };

        // No column refs to check, so no NULLs possible
        assert!(!has_null_values(&expr, &rows, &evaluator));
    }
}
