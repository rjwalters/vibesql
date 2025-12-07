//! Pivot aggregate optimization for batched SUM(CASE...) evaluation
//!
//! This module optimizes the common SQL pivot pattern where multiple aggregates
//! compare the same column against different literal values:
//!
//! ```sql
//! SELECT
//!     SUM(CASE WHEN d_day_name = 'Sunday' THEN sales_price ELSE NULL END) sun_sales,
//!     SUM(CASE WHEN d_day_name = 'Monday' THEN sales_price ELSE NULL END) mon_sales,
//!     ...
//! FROM ...
//! GROUP BY ...
//! ```
//!
//! Instead of evaluating each aggregate separately (7 passes through the data),
//! this optimization:
//! 1. Detects the pivot pattern across multiple SELECT items
//! 2. Executes all related aggregates in a single pass
//! 3. Caches all results for subsequent aggregate evaluations
//!
//! Performance impact: ~3.5x speedup for TPC-DS Q2 style queries with 7+ pivot aggregates.

use std::collections::HashMap;

use crate::schema::CombinedSchema;
use crate::select::grouping::AggregateAccumulator;
use vibesql_ast::Expression;
use vibesql_types::SqlValue;

/// A group of pivot aggregates that can be evaluated together in a single pass.
///
/// All aggregates in the group share:
/// - Same aggregate function (e.g., SUM)
/// - Same condition column (e.g., d_day_name)
/// - Same result column (e.g., sales_price)
/// - Different condition values (e.g., 'Sunday', 'Monday', ...)
#[derive(Debug)]
pub struct PivotAggregateGroup {
    /// The aggregate function name (e.g., "SUM")
    pub function_name: String,
    /// Column index for the condition column (e.g., d_day_name)
    pub condition_col_idx: usize,
    /// Column index for the value column (e.g., sales_price)
    pub value_col_idx: usize,
    /// Whether DISTINCT is used
    pub distinct: bool,
    /// Maps condition value -> (cache_key, result) after execution
    /// The cache_key is used to store results in the aggregate cache
    pub entries: Vec<PivotEntry>,
}

/// A single entry in a pivot aggregate group
#[derive(Debug)]
pub struct PivotEntry {
    /// The condition value to match (e.g., 'Sunday')
    pub condition_value: SqlValue,
    /// The cache key for storing results (matches aggregate_function::evaluate format)
    pub cache_key: String,
}

impl PivotAggregateGroup {
    /// Try to detect a pivot pattern in the SELECT list.
    ///
    /// Returns Some if 2+ aggregates share the same pattern, None otherwise.
    /// The minimum threshold of 2 is chosen because even 2 aggregates benefit
    /// from single-pass evaluation (reduces 2 passes to 1).
    pub fn try_detect(
        select_list: &[vibesql_ast::SelectItem],
        schema: &CombinedSchema,
    ) -> Option<Self> {
        let mut candidates: Vec<PivotCandidate> = Vec::new();

        // Scan SELECT list for SUM(CASE WHEN col = literal THEN result_col ELSE NULL END)
        for item in select_list {
            if let vibesql_ast::SelectItem::Expression { expr, .. } = item {
                if let Some(candidate) = Self::try_extract_pivot_candidate(expr, schema) {
                    candidates.push(candidate);
                }
            }
        }

        // Need at least 2 candidates with matching structure to form a pivot group
        if candidates.len() < 2 {
            return None;
        }

        // Group candidates by (function_name, condition_col_idx, value_col_idx, distinct)
        // Take the largest group with 2+ members
        let mut groups: HashMap<(String, usize, usize, bool), Vec<PivotCandidate>> = HashMap::new();

        for candidate in candidates {
            let key = (
                candidate.function_name.clone(),
                candidate.condition_col_idx,
                candidate.value_col_idx,
                candidate.distinct,
            );
            groups.entry(key).or_default().push(candidate);
        }

        // Find the largest group with 2+ members
        let best_group =
            groups.into_iter().filter(|(_, v)| v.len() >= 2).max_by_key(|(_, v)| v.len());

        best_group.map(
            |((function_name, condition_col_idx, value_col_idx, distinct), candidates)| {
                let entries = candidates
                    .into_iter()
                    .map(|c| PivotEntry {
                        condition_value: c.condition_value,
                        cache_key: c.cache_key,
                    })
                    .collect();

                PivotAggregateGroup {
                    function_name,
                    condition_col_idx,
                    value_col_idx,
                    distinct,
                    entries,
                }
            },
        )
    }

    /// Extract a pivot candidate from an expression if it matches the pattern:
    /// AGG(CASE WHEN col = literal THEN result_col ELSE NULL END)
    fn try_extract_pivot_candidate(
        expr: &Expression,
        schema: &CombinedSchema,
    ) -> Option<PivotCandidate> {
        // Must be an aggregate function
        let (name, distinct, args) = match expr {
            Expression::AggregateFunction { name, distinct, args } => (name, *distinct, args),
            _ => return None,
        };

        // Must have exactly 1 argument
        if args.len() != 1 {
            return None;
        }

        // Argument must be a CASE expression
        let (operand, when_clauses, else_result) = match &args[0] {
            Expression::Case { operand, when_clauses, else_result } => {
                (operand, when_clauses, else_result)
            }
            _ => return None,
        };

        // Must be a searched CASE (no operand)
        if operand.is_some() {
            return None;
        }

        // Must have exactly 1 WHEN clause
        if when_clauses.len() != 1 {
            return None;
        }

        // ELSE must be NULL or absent
        match else_result.as_deref() {
            None => {}
            Some(Expression::Literal(SqlValue::Null)) => {}
            _ => return None,
        }

        let when_clause = &when_clauses[0];

        // Must have exactly 1 condition
        if when_clause.conditions.len() != 1 {
            return None;
        }

        let condition = &when_clause.conditions[0];
        let result = &when_clause.result;

        // Condition must be col = literal
        let (condition_col_idx, condition_value) = match condition {
            Expression::BinaryOp { op: vibesql_ast::BinaryOperator::Equal, left, right } => {
                // Try left = column, right = literal
                if let Some(pair) = Self::extract_column_equals_literal(left, right, schema) {
                    pair
                } else if let Some(pair) = Self::extract_column_equals_literal(right, left, schema)
                {
                    // Try right = column, left = literal
                    pair
                } else {
                    return None;
                }
            }
            _ => return None,
        };

        // Result must be a column reference (value column)
        let value_col_idx = match result {
            Expression::ColumnRef { table, column } => {
                schema.get_column_index(table.as_deref(), column)?
            }
            _ => return None,
        };

        // Generate cache key matching aggregate_function::evaluate format
        let cache_key = format!("{}:{}:{:?}", name.to_uppercase(), distinct, args);

        Some(PivotCandidate {
            function_name: name.to_uppercase(),
            condition_col_idx,
            condition_value,
            value_col_idx,
            distinct,
            cache_key,
        })
    }

    /// Extract column index and literal value from column = literal pattern
    fn extract_column_equals_literal(
        col_expr: &Expression,
        lit_expr: &Expression,
        schema: &CombinedSchema,
    ) -> Option<(usize, SqlValue)> {
        let col_idx = match col_expr {
            Expression::ColumnRef { table, column } => {
                schema.get_column_index(table.as_deref(), column)?
            }
            _ => return None,
        };

        let lit_val = match lit_expr {
            Expression::Literal(val) => val.clone(),
            _ => return None,
        };

        Some((col_idx, lit_val))
    }

    /// Execute all pivot aggregates in a single pass over the rows.
    ///
    /// Returns a map from cache_key to finalized result.
    pub fn execute(
        &self,
        group_rows: &[vibesql_storage::Row],
    ) -> Result<HashMap<String, SqlValue>, crate::errors::ExecutorError> {
        // Create accumulators for each entry
        let mut accumulators: Vec<AggregateAccumulator> = self
            .entries
            .iter()
            .map(|_| AggregateAccumulator::new(&self.function_name, self.distinct))
            .collect::<Result<Vec<_>, _>>()?;

        // Build a lookup map from condition_value -> accumulator index
        let mut value_to_idx: HashMap<&SqlValue, usize> =
            HashMap::with_capacity(self.entries.len());
        for (idx, entry) in self.entries.iter().enumerate() {
            value_to_idx.insert(&entry.condition_value, idx);
        }

        // Single pass through all rows
        for row in group_rows {
            // Read condition column once per row
            let condition_val = &row.values[self.condition_col_idx];

            // Look up which accumulator (if any) should receive this value
            if let Some(&acc_idx) = value_to_idx.get(condition_val) {
                // Read value column once per row (only if we have a matching condition)
                let value = &row.values[self.value_col_idx];
                accumulators[acc_idx].accumulate(value);
            }
        }

        // Finalize and build result map
        let mut results = HashMap::with_capacity(self.entries.len());
        for (entry, acc) in self.entries.iter().zip(accumulators.into_iter()) {
            results.insert(entry.cache_key.clone(), acc.finalize());
        }

        Ok(results)
    }
}

/// Internal candidate structure used during pattern detection
#[derive(Debug)]
struct PivotCandidate {
    function_name: String,
    condition_col_idx: usize,
    condition_value: SqlValue,
    value_col_idx: usize,
    distinct: bool,
    cache_key: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_ast::{BinaryOperator, CaseWhen, SelectItem};
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_storage::Row;
    use vibesql_types::DataType;

    fn create_test_schema() -> CombinedSchema {
        let table_schema = TableSchema::new(
            "test".to_string(),
            vec![
                ColumnSchema::new(
                    "d_day_name".to_string(),
                    DataType::Varchar { max_length: Some(20) },
                    false,
                ),
                ColumnSchema::new("sales_price".to_string(), DataType::Integer, false),
                ColumnSchema::new("d_week_seq".to_string(), DataType::Integer, false),
            ],
        );
        CombinedSchema::from_table("test".to_string(), table_schema)
    }

    fn create_sum_case_expression(match_value: &str) -> Expression {
        // SUM(CASE WHEN d_day_name = match_value THEN sales_price ELSE NULL END)
        Expression::AggregateFunction {
            name: "SUM".to_string(),
            distinct: false,
            args: vec![Expression::Case {
                operand: None,
                when_clauses: vec![CaseWhen {
                    conditions: vec![Expression::BinaryOp {
                        left: Box::new(Expression::ColumnRef {
                            table: None,
                            column: "d_day_name".to_string(),
                        }),
                        op: BinaryOperator::Equal,
                        right: Box::new(Expression::Literal(SqlValue::Varchar(
                            arcstr::ArcStr::from(match_value),
                        ))),
                    }],
                    result: Expression::ColumnRef {
                        table: None,
                        column: "sales_price".to_string(),
                    },
                }],
                else_result: Some(Box::new(Expression::Literal(SqlValue::Null))),
            }],
        }
    }

    #[test]
    fn test_detect_pivot_pattern_with_two_aggregates() {
        let schema = create_test_schema();

        let select_list = vec![
            SelectItem::Expression {
                expr: create_sum_case_expression("Sunday"),
                alias: Some("sun_sales".to_string()),
            },
            SelectItem::Expression {
                expr: create_sum_case_expression("Monday"),
                alias: Some("mon_sales".to_string()),
            },
        ];

        let pivot = PivotAggregateGroup::try_detect(&select_list, &schema);
        assert!(pivot.is_some(), "Should detect pivot pattern with 2 aggregates");

        let pivot = pivot.unwrap();
        assert_eq!(pivot.function_name, "SUM");
        assert_eq!(pivot.condition_col_idx, 0); // d_day_name
        assert_eq!(pivot.value_col_idx, 1); // sales_price
        assert_eq!(pivot.entries.len(), 2);
    }

    #[test]
    fn test_detect_pivot_pattern_with_seven_aggregates() {
        let schema = create_test_schema();

        let days = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];
        let select_list: Vec<SelectItem> = days
            .iter()
            .map(|day| SelectItem::Expression {
                expr: create_sum_case_expression(day),
                alias: Some(format!("{}_sales", day.to_lowercase())),
            })
            .collect();

        let pivot = PivotAggregateGroup::try_detect(&select_list, &schema);
        assert!(pivot.is_some(), "Should detect pivot pattern with 7 aggregates");

        let pivot = pivot.unwrap();
        assert_eq!(pivot.entries.len(), 7);
    }

    #[test]
    fn test_no_pivot_with_single_aggregate() {
        let schema = create_test_schema();

        let select_list = vec![SelectItem::Expression {
            expr: create_sum_case_expression("Sunday"),
            alias: Some("sun_sales".to_string()),
        }];

        let pivot = PivotAggregateGroup::try_detect(&select_list, &schema);
        assert!(pivot.is_none(), "Should not detect pivot with single aggregate");
    }

    #[test]
    fn test_no_pivot_with_different_condition_columns() {
        let schema = create_test_schema();

        // First aggregate: SUM(CASE WHEN d_day_name = 'Sunday' THEN sales_price ...)
        let expr1 = create_sum_case_expression("Sunday");

        // Second aggregate: SUM(CASE WHEN d_week_seq = 1 THEN sales_price ...)
        // Different condition column
        let expr2 = Expression::AggregateFunction {
            name: "SUM".to_string(),
            distinct: false,
            args: vec![Expression::Case {
                operand: None,
                when_clauses: vec![CaseWhen {
                    conditions: vec![Expression::BinaryOp {
                        left: Box::new(Expression::ColumnRef {
                            table: None,
                            column: "d_week_seq".to_string(),
                        }),
                        op: BinaryOperator::Equal,
                        right: Box::new(Expression::Literal(SqlValue::Integer(1))),
                    }],
                    result: Expression::ColumnRef {
                        table: None,
                        column: "sales_price".to_string(),
                    },
                }],
                else_result: Some(Box::new(Expression::Literal(SqlValue::Null))),
            }],
        };

        let select_list = vec![
            SelectItem::Expression { expr: expr1, alias: Some("sun_sales".to_string()) },
            SelectItem::Expression { expr: expr2, alias: Some("week1_sales".to_string()) },
        ];

        let pivot = PivotAggregateGroup::try_detect(&select_list, &schema);
        assert!(pivot.is_none(), "Should not detect pivot with different condition columns");
    }

    #[test]
    fn test_execute_pivot_aggregates() {
        let schema = create_test_schema();

        let select_list = vec![
            SelectItem::Expression {
                expr: create_sum_case_expression("Sunday"),
                alias: Some("sun_sales".to_string()),
            },
            SelectItem::Expression {
                expr: create_sum_case_expression("Monday"),
                alias: Some("mon_sales".to_string()),
            },
            SelectItem::Expression {
                expr: create_sum_case_expression("Tuesday"),
                alias: Some("tue_sales".to_string()),
            },
        ];

        let pivot = PivotAggregateGroup::try_detect(&select_list, &schema).unwrap();

        // Create test data
        let rows = vec![
            Row::new(vec![
                SqlValue::Varchar(arcstr::ArcStr::from("Sunday")),
                SqlValue::Integer(100),
                SqlValue::Integer(1),
            ]),
            Row::new(vec![
                SqlValue::Varchar(arcstr::ArcStr::from("Sunday")),
                SqlValue::Integer(200),
                SqlValue::Integer(1),
            ]),
            Row::new(vec![
                SqlValue::Varchar(arcstr::ArcStr::from("Monday")),
                SqlValue::Integer(50),
                SqlValue::Integer(1),
            ]),
            Row::new(vec![
                SqlValue::Varchar(arcstr::ArcStr::from("Wednesday")), // Not in pivot
                SqlValue::Integer(999),
                SqlValue::Integer(1),
            ]),
        ];

        let results = pivot.execute(&rows).unwrap();

        // Find results by checking cache keys
        let sun_key = pivot
            .entries
            .iter()
            .find(|e| matches!(&e.condition_value, SqlValue::Varchar(s) if s.as_str() == "Sunday"))
            .unwrap();
        let mon_key = pivot
            .entries
            .iter()
            .find(|e| matches!(&e.condition_value, SqlValue::Varchar(s) if s.as_str() == "Monday"))
            .unwrap();
        let tue_key = pivot
            .entries
            .iter()
            .find(|e| matches!(&e.condition_value, SqlValue::Varchar(s) if s.as_str() == "Tuesday"))
            .unwrap();

        assert_eq!(results.get(&sun_key.cache_key), Some(&SqlValue::Integer(300))); // 100 + 200
        assert_eq!(results.get(&mon_key.cache_key), Some(&SqlValue::Integer(50)));
        assert_eq!(results.get(&tue_key.cache_key), Some(&SqlValue::Null)); // No Tuesday rows
    }

    #[test]
    fn test_execute_with_empty_rows() {
        let schema = create_test_schema();

        let select_list = vec![
            SelectItem::Expression {
                expr: create_sum_case_expression("Sunday"),
                alias: Some("sun_sales".to_string()),
            },
            SelectItem::Expression {
                expr: create_sum_case_expression("Monday"),
                alias: Some("mon_sales".to_string()),
            },
        ];

        let pivot = PivotAggregateGroup::try_detect(&select_list, &schema).unwrap();
        let results = pivot.execute(&[]).unwrap();

        // All should be NULL for empty input
        for entry in &pivot.entries {
            assert_eq!(results.get(&entry.cache_key), Some(&SqlValue::Null));
        }
    }
}
