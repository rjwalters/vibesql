//! Filter predicate extraction and evaluation
//!
//! This module provides infrastructure for extracting and efficiently evaluating
//! filter predicates from WHERE clauses.

use std::str::FromStr;

use vibesql_ast::{BinaryOperator, Expression};
use vibesql_storage::Row;
use vibesql_types::{DataType, Date, SqlValue};

use crate::schema::CombinedSchema;

/// Represents a filter predicate that can be evaluated efficiently
#[derive(Debug, Clone)]
pub enum FilterPredicate {
    /// Date range filter: column >= min AND column < max
    DateRange {
        column_idx: usize,
        min: Date,
        max: Date,
    },
    /// Numeric BETWEEN filter: column >= min AND column <= max
    Between {
        column_idx: usize,
        min: f64,
        max: f64,
    },
    /// Comparison with constant: column OP value
    Comparison {
        column_idx: usize,
        op: ComparisonOp,
        value: SqlValue,
        value_type: DataType,
    },
}

/// Comparison operators supported in filter predicates
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComparisonOp {
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    Equal,
    NotEqual,
}

impl FilterPredicate {
    /// Evaluate this filter predicate on a row
    ///
    /// # Safety
    ///
    /// Uses unchecked accessors for performance. Safe because column indices
    /// and types are validated at plan creation time.
    #[inline(always)]
    pub unsafe fn evaluate(&self, row: &Row) -> bool {
        match self {
            FilterPredicate::DateRange {
                column_idx,
                min,
                max,
            } => {
                let value = row.get_date_unchecked(*column_idx);
                value >= *min && value < *max
            }
            FilterPredicate::Between {
                column_idx,
                min,
                max,
            } => {
                let value = row.get_numeric_as_f64_unchecked(*column_idx);
                value >= *min && value <= *max
            }
            FilterPredicate::Comparison {
                column_idx,
                op,
                value,
                value_type,
            } => Self::evaluate_comparison(row, *column_idx, *op, value, value_type),
        }
    }

    /// Evaluate a comparison predicate
    #[inline(always)]
    unsafe fn evaluate_comparison(
        row: &Row,
        column_idx: usize,
        op: ComparisonOp,
        value: &SqlValue,
        value_type: &DataType,
    ) -> bool {
        match value_type {
            DataType::DoublePrecision | DataType::Real | DataType::Decimal { .. } => {
                let row_value = row.get_f64_unchecked(column_idx);
                let compare_value = match value {
                    SqlValue::Double(v) => *v,
                    SqlValue::Integer(v) => *v as f64,
                    _ => return false,
                };
                Self::compare_f64(row_value, op, compare_value)
            }
            DataType::Integer | DataType::Bigint | DataType::Smallint | DataType::Unsigned => {
                let row_value = row.get_i64_unchecked(column_idx);
                let compare_value = match value {
                    SqlValue::Integer(v) | SqlValue::Bigint(v) => *v,
                    SqlValue::Smallint(v) => *v as i64,
                    _ => return false,
                };
                Self::compare_i64(row_value, op, compare_value)
            }
            DataType::Date => {
                let row_value = row.get_date_unchecked(column_idx);
                let compare_value = match value {
                    SqlValue::Date(d) => *d,
                    SqlValue::Varchar(s) => {
                        // Parse Varchar date string (e.g., "2024-01-01")
                        match Date::from_str(s) {
                            Ok(d) => d,
                            Err(_) => return false,
                        }
                    }
                    _ => return false,
                };
                Self::compare_date(row_value, op, compare_value)
            }
            DataType::Boolean => {
                let row_value = row.get_bool_unchecked(column_idx);
                let compare_value = match value {
                    SqlValue::Boolean(b) => *b,
                    _ => return false,
                };
                Self::compare_bool(row_value, op, compare_value)
            }
            _ => false,
        }
    }

    #[inline(always)]
    fn compare_f64(a: f64, op: ComparisonOp, b: f64) -> bool {
        match op {
            ComparisonOp::LessThan => a < b,
            ComparisonOp::LessThanOrEqual => a <= b,
            ComparisonOp::GreaterThan => a > b,
            ComparisonOp::GreaterThanOrEqual => a >= b,
            ComparisonOp::Equal => (a - b).abs() < f64::EPSILON,
            ComparisonOp::NotEqual => (a - b).abs() >= f64::EPSILON,
        }
    }

    #[inline(always)]
    fn compare_i64(a: i64, op: ComparisonOp, b: i64) -> bool {
        match op {
            ComparisonOp::LessThan => a < b,
            ComparisonOp::LessThanOrEqual => a <= b,
            ComparisonOp::GreaterThan => a > b,
            ComparisonOp::GreaterThanOrEqual => a >= b,
            ComparisonOp::Equal => a == b,
            ComparisonOp::NotEqual => a != b,
        }
    }

    #[inline(always)]
    fn compare_date(a: Date, op: ComparisonOp, b: Date) -> bool {
        match op {
            ComparisonOp::LessThan => a < b,
            ComparisonOp::LessThanOrEqual => a <= b,
            ComparisonOp::GreaterThan => a > b,
            ComparisonOp::GreaterThanOrEqual => a >= b,
            ComparisonOp::Equal => a == b,
            ComparisonOp::NotEqual => a != b,
        }
    }

    #[inline(always)]
    fn compare_bool(a: bool, op: ComparisonOp, b: bool) -> bool {
        match op {
            ComparisonOp::Equal => a == b,
            ComparisonOp::NotEqual => a != b,
            _ => false, // Other comparisons don't make sense for booleans
        }
    }
}

/// Helper to get column type by name from schema
pub(super) fn get_column_type(schema: &CombinedSchema, column_name: &str) -> Option<DataType> {
    // Search all tables for the column
    for (_start_index, table_schema) in schema.table_schemas.values() {
        if let Some(col) = table_schema.get_column(column_name) {
            return Some(col.data_type.clone());
        }
    }
    None
}

/// Extract filter predicates from a WHERE clause
///
/// Returns None if filters cannot be extracted (e.g., unsupported expression types)
pub fn extract_filters(
    where_clause: &Option<Expression>,
    schema: &CombinedSchema,
) -> Option<Vec<FilterPredicate>> {
    match where_clause {
        Some(expr) => extract_filters_from_expr(expr, schema),
        None => Some(vec![]),
    }
}

/// Recursively extract filters from an expression
fn extract_filters_from_expr(
    expr: &Expression,
    schema: &CombinedSchema,
) -> Option<Vec<FilterPredicate>> {
    match expr {
        // AND: combine filters from both sides
        Expression::BinaryOp {
            op: BinaryOperator::And,
            left,
            right,
        } => {
            let mut left_filters = extract_filters_from_expr(left, schema)?;
            let right_filters = extract_filters_from_expr(right, schema)?;
            left_filters.extend(right_filters);
            Some(left_filters)
        }

        // BETWEEN: extract as Between predicate
        Expression::Between {
            expr: inner_expr,
            low,
            high,
            negated: false,
            ..
        } => {
            // Must be column BETWEEN literal AND literal
            let column_name = match inner_expr.as_ref() {
                Expression::ColumnRef { column, .. } => column,
                _ => return None,
            };

            let column_idx = schema.get_column_index(None, column_name)?;
            let column_type = get_column_type(schema, column_name)?;

            // Try to extract as numeric BETWEEN
            if matches!(
                column_type,
                DataType::DoublePrecision
                    | DataType::Real
                    | DataType::Decimal { .. }
                    | DataType::Integer
                    | DataType::Bigint
                    | DataType::Unsigned
            ) {
                let low_val = extract_f64_literal(low)?;
                let high_val = extract_f64_literal(high)?;
                return Some(vec![FilterPredicate::Between {
                    column_idx,
                    min: low_val,
                    max: high_val,
                }]);
            }

            None
        }

        // Comparison: column OP literal
        Expression::BinaryOp { op, left, right } => {
            let comparison_op = match op {
                BinaryOperator::LessThan => ComparisonOp::LessThan,
                BinaryOperator::LessThanOrEqual => ComparisonOp::LessThanOrEqual,
                BinaryOperator::GreaterThan => ComparisonOp::GreaterThan,
                BinaryOperator::GreaterThanOrEqual => ComparisonOp::GreaterThanOrEqual,
                BinaryOperator::Equal => ComparisonOp::Equal,
                BinaryOperator::NotEqual => ComparisonOp::NotEqual,
                _ => return None,
            };

            // Try column OP literal
            if let Expression::ColumnRef { column, .. } = left.as_ref() {
                if let Expression::Literal(value) = right.as_ref() {
                    let column_idx = schema.get_column_index(None, column)?;
                    let column_type = get_column_type(schema, column)?;

                    // Special case: detect date range patterns (col >= date1 AND col < date2)
                    if column_type == DataType::Date {
                        if let SqlValue::Date(_date) = value {
                            // This might be part of a date range, but we'll just add it as a comparison
                            // Date range optimization will happen if we find matching predicates
                            return Some(vec![FilterPredicate::Comparison {
                                column_idx,
                                op: comparison_op,
                                value: value.clone(),
                                value_type: column_type,
                            }]);
                        }
                    }

                    return Some(vec![FilterPredicate::Comparison {
                        column_idx,
                        op: comparison_op,
                        value: value.clone(),
                        value_type: column_type,
                    }]);
                }
            }

            // Try literal OP column (reverse the operator)
            if let Expression::Literal(value) = left.as_ref() {
                if let Expression::ColumnRef { column, .. } = right.as_ref() {
                    let column_idx = schema.get_column_index(None, column)?;
                    let column_type = get_column_type(schema, column)?;

                    // Reverse the operator
                    let reversed_op = match comparison_op {
                        ComparisonOp::LessThan => ComparisonOp::GreaterThan,
                        ComparisonOp::LessThanOrEqual => ComparisonOp::GreaterThanOrEqual,
                        ComparisonOp::GreaterThan => ComparisonOp::LessThan,
                        ComparisonOp::GreaterThanOrEqual => ComparisonOp::LessThanOrEqual,
                        ComparisonOp::Equal => ComparisonOp::Equal,
                        ComparisonOp::NotEqual => ComparisonOp::NotEqual,
                    };

                    return Some(vec![FilterPredicate::Comparison {
                        column_idx,
                        op: reversed_op,
                        value: value.clone(),
                        value_type: column_type,
                    }]);
                }
            }

            None
        }

        _ => None,
    }
}

/// Helper to extract an f64 literal from an expression
fn extract_f64_literal(expr: &Expression) -> Option<f64> {
    match expr {
        Expression::Literal(SqlValue::Double(v)) => Some(*v),
        Expression::Literal(SqlValue::Integer(v)) => Some(*v as f64),
        Expression::Literal(SqlValue::Numeric(n)) => {
            // Convert Numeric (rust_decimal) to f64
            n.to_string().parse::<f64>().ok()
        }
        _ => None,
    }
}

/// Optimize date range filters by combining adjacent date comparisons
///
/// Looks for patterns like: col >= date1 AND col < date2
/// and combines them into a single DateRange predicate
pub fn optimize_date_ranges(filters: Vec<FilterPredicate>) -> Vec<FilterPredicate> {
    let mut optimized = Vec::new();
    let mut i = 0;

    while i < filters.len() {
        // Look for date comparison pairs
        if i + 1 < filters.len() {
            if let (
                FilterPredicate::Comparison {
                    column_idx: idx1,
                    op: op1,
                    value: SqlValue::Date(date1),
                    value_type: DataType::Date,
                },
                FilterPredicate::Comparison {
                    column_idx: idx2,
                    op: op2,
                    value: SqlValue::Date(date2),
                    value_type: DataType::Date,
                },
            ) = (&filters[i], &filters[i + 1])
            {
                // Same column?
                if idx1 == idx2 {
                    // Check for range pattern: col >= min AND col < max
                    match (op1, op2) {
                        (ComparisonOp::GreaterThanOrEqual, ComparisonOp::LessThan)
                        | (ComparisonOp::GreaterThan, ComparisonOp::LessThanOrEqual) => {
                            optimized.push(FilterPredicate::DateRange {
                                column_idx: *idx1,
                                min: *date1,
                                max: *date2,
                            });
                            i += 2;
                            continue;
                        }
                        (ComparisonOp::LessThan, ComparisonOp::GreaterThanOrEqual)
                        | (ComparisonOp::LessThanOrEqual, ComparisonOp::GreaterThan) => {
                            optimized.push(FilterPredicate::DateRange {
                                column_idx: *idx1,
                                min: *date2,
                                max: *date1,
                            });
                            i += 2;
                            continue;
                        }
                        _ => {}
                    }
                }
            }
        }

        // No optimization, just add the filter
        optimized.push(filters[i].clone());
        i += 1;
    }

    optimized
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_parser::Parser;
    use vibesql_ast::SelectStmt;

    fn parse_select_query(query: &str) -> SelectStmt {
        let stmt = Parser::parse_sql(query).expect("Failed to parse SQL");
        match stmt {
            vibesql_ast::Statement::Select(select_stmt) => *select_stmt,
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_filter_extraction_simple_comparison() {
        // Create schema with a sales table
        let table = TableSchema::new(
            "sales".to_string(),
            vec![
                ColumnSchema::new("price".to_string(), DataType::DoublePrecision, true),
                ColumnSchema::new("quantity".to_string(), DataType::Integer, true),
            ],
        );
        let schema = CombinedSchema::from_table("sales".to_string(), table);

        // Parse query with simple comparison
        let query = "SELECT * FROM sales WHERE quantity < 100";
        let stmt = parse_select_query(query);

        // Extract filters
        let filters_opt = extract_filters(&stmt.where_clause, &schema);
        assert!(filters_opt.is_some(), "Should be able to extract filters");
        let filters = filters_opt.unwrap();
        assert_eq!(filters.len(), 1);

        match &filters[0] {
            FilterPredicate::Comparison {
                column_idx,
                op,
                value,
                ..
            } => {
                assert_eq!(*column_idx, 1); // quantity is column 1
                assert_eq!(*op, ComparisonOp::LessThan);
                assert_eq!(*value, SqlValue::Integer(100));
            }
            _ => panic!("Expected Comparison filter"),
        }
    }
}
