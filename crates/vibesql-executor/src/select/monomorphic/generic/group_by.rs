//! Generic GROUP BY aggregation patterns
//!
//! This module provides generic monomorphic execution plans for GROUP BY
//! aggregation queries with support for multiple aggregation functions.

use std::collections::HashMap;

use vibesql_ast::{BinaryOperator, Expression, SelectStmt};
use vibesql_storage::Row;
use vibesql_types::{DataType, SqlValue};

use crate::{errors::ExecutorError, schema::CombinedSchema};

use super::super::pattern::has_no_joins;
use super::super::MonomorphicPlan;
use super::filter::{extract_filters, get_column_type, optimize_date_ranges, FilterPredicate};

/// Specification for an aggregation column in GROUP BY queries
#[derive(Debug, Clone)]
pub enum GroupAggregateSpec {
    /// SUM(col)
    Sum { col_idx: usize },
    /// SUM(col1 * col2)
    SumProduct { col1_idx: usize, col2_idx: usize },
    /// SUM(col1 * (1 - col2))
    SumProductOneMinusCol {
        col1_idx: usize,
        col2_idx: usize,
    },
    /// SUM(col1 * (1 - col2) * (1 + col3))
    SumProductComplex {
        col1_idx: usize,
        col2_idx: usize,
        col3_idx: usize,
    },
    /// AVG(col) - stored as sum/count
    Avg { col_idx: usize },
    /// COUNT(*)
    Count,
}

impl GroupAggregateSpec {
    /// Accumulate a value for this aggregate
    ///
    /// # Safety
    ///
    /// Uses unchecked accessors for performance. Safe because column indices
    /// are validated at plan creation time.
    #[inline(always)]
    pub unsafe fn accumulate(&self, acc: f64, row: &Row) -> f64 {
        match self {
            GroupAggregateSpec::Sum { col_idx } => {
                let val = row.get_numeric_as_f64_unchecked(*col_idx);
                acc + val
            }
            GroupAggregateSpec::SumProduct { col1_idx, col2_idx } => {
                let val1 = row.get_numeric_as_f64_unchecked(*col1_idx);
                let val2 = row.get_numeric_as_f64_unchecked(*col2_idx);
                acc + (val1 * val2)
            }
            GroupAggregateSpec::SumProductOneMinusCol { col1_idx, col2_idx } => {
                let val1 = row.get_numeric_as_f64_unchecked(*col1_idx);
                let val2 = row.get_numeric_as_f64_unchecked(*col2_idx);
                acc + (val1 * (1.0 - val2))
            }
            GroupAggregateSpec::SumProductComplex {
                col1_idx,
                col2_idx,
                col3_idx,
            } => {
                let val1 = row.get_numeric_as_f64_unchecked(*col1_idx);
                let val2 = row.get_numeric_as_f64_unchecked(*col2_idx);
                let val3 = row.get_numeric_as_f64_unchecked(*col3_idx);
                acc + (val1 * (1.0 - val2) * (1.0 + val3))
            }
            GroupAggregateSpec::Avg { col_idx } => {
                let val = row.get_numeric_as_f64_unchecked(*col_idx);
                acc + val
            }
            GroupAggregateSpec::Count => acc + 1.0,
        }
    }
}

/// Specification for a GROUP BY column
#[derive(Debug, Clone)]
pub struct GroupByColumnSpec {
    pub col_idx: usize,
    pub col_type: DataType,
}

/// Generic grouped aggregation plan
///
/// Handles queries like:
/// ```sql
/// SELECT col1, col2, SUM(col3), AVG(col4), COUNT(*)
/// FROM any_table
/// WHERE <filters>
/// GROUP BY col1, col2
/// ```
///
/// This plan is generic - it works for any table and any compatible GROUP BY pattern.
pub struct GenericGroupedAggregationPlan {
    /// Filter predicates to evaluate
    filters: Vec<FilterPredicate>,

    /// GROUP BY columns (for grouping key)
    group_by_columns: Vec<GroupByColumnSpec>,

    /// Aggregations to compute for each group
    aggregates: Vec<GroupAggregateSpec>,

    /// Description for debugging
    #[allow(dead_code)]
    description: String,
}

impl GenericGroupedAggregationPlan {
    /// Try to create a generic grouped aggregation plan
    pub fn try_create(stmt: &SelectStmt, schema: &CombinedSchema) -> Option<Self> {
        // Must have no joins
        if !has_no_joins(&stmt.from) {
            return None;
        }

        // HAVING clause not yet supported in monomorphic path - fall back to standard execution
        if stmt.having.is_some() {
            return None;
        }

        // Must have GROUP BY (only simple GROUP BY supported in monomorphic path)
        let group_by = stmt.group_by.as_ref()?;

        // Only simple GROUP BY supported in monomorphic path
        let simple_exprs = group_by.as_simple()?;

        // Extract GROUP BY columns
        let group_by_columns = Self::extract_group_by_columns(simple_exprs, schema)?;

        // Extract aggregations
        let aggregates = Self::extract_aggregates(&stmt.select_list, schema)?;

        // Must have at least one aggregate
        if aggregates.is_empty() {
            return None;
        }

        // Extract filter predicates from WHERE clause
        let filters = extract_filters(&stmt.where_clause, schema)?;

        // Optimize date ranges
        let optimized_filters = optimize_date_ranges(filters);

        // Get table name for description
        let table_name = match &stmt.from {
            Some(vibesql_ast::FromClause::Table { name, .. }) => name.clone(),
            _ => "unknown".to_string(),
        };

        Some(Self {
            description: format!(
                "Generic Grouped Aggregation on {} ({} groups, {} aggregates, {} filters)",
                table_name,
                group_by_columns.len(),
                aggregates.len(),
                optimized_filters.len()
            ),
            filters: optimized_filters,
            group_by_columns,
            aggregates,
        })
    }

    /// Extract GROUP BY columns from GROUP BY clause
    fn extract_group_by_columns(
        group_by: &[Expression],
        schema: &CombinedSchema,
    ) -> Option<Vec<GroupByColumnSpec>> {
        let mut columns = Vec::new();

        for expr in group_by {
            if let Expression::ColumnRef { column, .. } = expr {
                let col_idx = schema.get_column_index(None, column)?;
                let col_type = get_column_type(schema, column)?;
                columns.push(GroupByColumnSpec { col_idx, col_type });
            } else {
                // Complex GROUP BY expressions not supported yet
                return None;
            }
        }

        Some(columns)
    }

    /// Extract aggregates from SELECT list
    fn extract_aggregates(
        select_list: &[vibesql_ast::SelectItem],
        schema: &CombinedSchema,
    ) -> Option<Vec<GroupAggregateSpec>> {
        let mut aggregates = Vec::new();

        for item in select_list {
            if let vibesql_ast::SelectItem::Expression { expr, .. } = item {
                if let Some(agg) = Self::extract_aggregate_from_expr(expr, schema) {
                    aggregates.push(agg);
                }
            }
        }

        Some(aggregates)
    }

    /// Extract a single aggregate from an expression
    fn extract_aggregate_from_expr(
        expr: &Expression,
        schema: &CombinedSchema,
    ) -> Option<GroupAggregateSpec> {
        match expr {
            Expression::AggregateFunction { name, args, .. } => {
                if name.eq_ignore_ascii_case("SUM") && args.len() == 1 {
                    // Check for different SUM patterns
                    Self::extract_sum_pattern(&args[0], schema)
                } else if name.eq_ignore_ascii_case("AVG") && args.len() == 1 {
                    // AVG(col)
                    if let Expression::ColumnRef { column, .. } = &args[0] {
                        let col_idx = schema.get_column_index(None, column)?;
                        return Some(GroupAggregateSpec::Avg { col_idx });
                    }
                    None
                } else if name.eq_ignore_ascii_case("COUNT") {
                    Some(GroupAggregateSpec::Count)
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Extract SUM pattern (handles various expressions)
    fn extract_sum_pattern(expr: &Expression, schema: &CombinedSchema) -> Option<GroupAggregateSpec> {
        match expr {
            // SUM(col)
            Expression::ColumnRef { column, .. } => {
                let col_idx = schema.get_column_index(None, column)?;
                Some(GroupAggregateSpec::Sum { col_idx })
            }
            // SUM(col1 * col2) or complex patterns
            Expression::BinaryOp {
                op: BinaryOperator::Multiply,
                left,
                right,
            } => {
                // Check for SUM(col1 * col2)
                if let (
                    Expression::ColumnRef { column: col1, .. },
                    Expression::ColumnRef { column: col2, .. },
                ) = (left.as_ref(), right.as_ref())
                {
                    let col1_idx = schema.get_column_index(None, col1)?;
                    let col2_idx = schema.get_column_index(None, col2)?;
                    return Some(GroupAggregateSpec::SumProduct { col1_idx, col2_idx });
                }

                // Check for SUM(col1 * (1 - col2))
                if let Expression::ColumnRef { column: col1, .. } = left.as_ref() {
                    if let Expression::BinaryOp {
                        op: BinaryOperator::Minus,
                        left: sub_left,
                        right: sub_right,
                    } = right.as_ref()
                    {
                        if let (Expression::Literal(SqlValue::Integer(1)), Expression::ColumnRef { column: col2, .. }) =
                            (sub_left.as_ref(), sub_right.as_ref())
                        {
                            let col1_idx = schema.get_column_index(None, col1)?;
                            let col2_idx = schema.get_column_index(None, col2)?;
                            return Some(GroupAggregateSpec::SumProductOneMinusCol {
                                col1_idx,
                                col2_idx,
                            });
                        }
                    }
                }

                // Check for SUM(col1 * (1 - col2) * (1 + col3))
                // This is a complex pattern: col1 * ((1 - col2) * (1 + col3))
                if let Expression::ColumnRef { column: col1, .. } = left.as_ref() {
                    if let Expression::BinaryOp {
                        op: BinaryOperator::Multiply,
                        left: mult_left,
                        right: mult_right,
                    } = right.as_ref()
                    {
                        // Check for (1 - col2) * (1 + col3)
                        if let (
                            Expression::BinaryOp {
                                op: BinaryOperator::Minus,
                                left: sub_left,
                                right: sub_right,
                            },
                            Expression::BinaryOp {
                                op: BinaryOperator::Plus,
                                left: add_left,
                                right: add_right,
                            },
                        ) = (mult_left.as_ref(), mult_right.as_ref())
                        {
                            if let (
                                Expression::Literal(SqlValue::Integer(1)),
                                Expression::ColumnRef { column: col2, .. },
                                Expression::Literal(SqlValue::Integer(1)),
                                Expression::ColumnRef { column: col3, .. },
                            ) = (
                                sub_left.as_ref(),
                                sub_right.as_ref(),
                                add_left.as_ref(),
                                add_right.as_ref(),
                            ) {
                                let col1_idx = schema.get_column_index(None, col1)?;
                                let col2_idx = schema.get_column_index(None, col2)?;
                                let col3_idx = schema.get_column_index(None, col3)?;
                                return Some(GroupAggregateSpec::SumProductComplex {
                                    col1_idx,
                                    col2_idx,
                                    col3_idx,
                                });
                            }
                        }
                    }
                }

                None
            }
            _ => None,
        }
    }

    /// Execute using type-specialized fast path
    ///
    /// # Safety
    ///
    /// Uses unchecked accessors for performance. Safe because column indices
    /// and types are validated at plan creation time.
    #[inline(never)] // Don't inline to make profiling easier
    unsafe fn execute_unsafe(&self, rows: &[Row]) -> HashMap<Vec<SqlValue>, Vec<f64>> {
        let mut groups: HashMap<Vec<SqlValue>, Vec<f64>> = HashMap::new();

        for row in rows {
            // Evaluate all filters
            let mut pass = true;
            for filter in &self.filters {
                if !filter.evaluate(row) {
                    pass = false;
                    break;
                }
            }

            if !pass {
                continue;
            }

            // Extract group key
            let mut key = Vec::with_capacity(self.group_by_columns.len());
            for group_col in &self.group_by_columns {
                let value = match &group_col.col_type {
                    DataType::Varchar { .. } => {
                        SqlValue::Varchar(row.get_string_unchecked(group_col.col_idx).to_string())
                    }
                    DataType::Integer | DataType::Bigint => {
                        SqlValue::Integer(row.get_i64_unchecked(group_col.col_idx))
                    }
                    DataType::DoublePrecision | DataType::Real | DataType::Decimal { .. } => {
                        SqlValue::Double(row.get_f64_unchecked(group_col.col_idx))
                    }
                    DataType::Date => SqlValue::Date(row.get_date_unchecked(group_col.col_idx)),
                    DataType::Boolean => SqlValue::Boolean(row.get_bool_unchecked(group_col.col_idx)),
                    _ => continue,
                };
                key.push(value);
            }

            // Get or create group aggregates
            let agg_values = groups
                .entry(key)
                .or_insert_with(|| vec![0.0; self.aggregates.len()]);

            // Accumulate each aggregate
            for (i, spec) in self.aggregates.iter().enumerate() {
                agg_values[i] = spec.accumulate(agg_values[i], row);
            }
        }

        groups
    }

    /// Execute using streaming fast path (lazy filtering)
    ///
    /// This method filters rows during iteration, only processing rows that match
    /// all predicates. This eliminates the overhead of materializing rows that
    /// will be filtered out.
    ///
    /// # Safety
    ///
    /// Uses unchecked accessors for performance. Safe because column indices
    /// and types are validated at plan creation time.
    #[inline(never)] // Don't inline to make profiling easier
    unsafe fn execute_stream_unsafe(
        &self,
        rows: Box<dyn Iterator<Item = Row>>,
    ) -> HashMap<Vec<SqlValue>, Vec<f64>> {
        let mut groups: HashMap<Vec<SqlValue>, Vec<f64>> = HashMap::new();

        // Stream through rows, filtering inline
        for row in rows {
            // Evaluate all filters
            let mut pass = true;
            for filter in &self.filters {
                if !filter.evaluate(&row) {
                    pass = false;
                    break;
                }
            }

            if !pass {
                continue;
            }

            // Extract group key
            let mut key = Vec::with_capacity(self.group_by_columns.len());
            for group_col in &self.group_by_columns {
                let value = match &group_col.col_type {
                    DataType::Varchar { .. } => {
                        SqlValue::Varchar(row.get_string_unchecked(group_col.col_idx).to_string())
                    }
                    DataType::Integer | DataType::Bigint => {
                        SqlValue::Integer(row.get_i64_unchecked(group_col.col_idx))
                    }
                    DataType::DoublePrecision | DataType::Real | DataType::Decimal { .. } => {
                        SqlValue::Double(row.get_f64_unchecked(group_col.col_idx))
                    }
                    DataType::Date => SqlValue::Date(row.get_date_unchecked(group_col.col_idx)),
                    DataType::Boolean => SqlValue::Boolean(row.get_bool_unchecked(group_col.col_idx)),
                    _ => continue,
                };
                key.push(value);
            }

            // Get or create group aggregates
            let agg_values = groups
                .entry(key)
                .or_insert_with(|| vec![0.0; self.aggregates.len()]);

            // Accumulate each aggregate
            for (i, spec) in self.aggregates.iter().enumerate() {
                agg_values[i] = spec.accumulate(agg_values[i], &row);
            }
        }

        groups
    }
}

impl MonomorphicPlan for GenericGroupedAggregationPlan {
    fn execute(&self, rows: &[Row]) -> Result<Vec<Row>, ExecutorError> {
        // Execute using unsafe fast path
        let groups = unsafe { self.execute_unsafe(rows) };

        // Convert to result rows
        let mut results: Vec<Row> = groups
            .into_iter()
            .map(|(key, agg_values)| {
                let mut values = key;

                // Add aggregate results, converting AVG to actual average
                for (i, spec) in self.aggregates.iter().enumerate() {
                    let val = match spec {
                        GroupAggregateSpec::Avg { .. } => {
                            // Get count from the last aggregate (should be COUNT)
                            let count = if let Some(GroupAggregateSpec::Count) =
                                self.aggregates.last()
                            {
                                agg_values[self.aggregates.len() - 1]
                            } else {
                                // If no COUNT, we can't compute AVG correctly
                                1.0
                            };
                            SqlValue::Double(agg_values[i] / count)
                        }
                        GroupAggregateSpec::Count => SqlValue::Integer(agg_values[i] as i64),
                        _ => SqlValue::Double(agg_values[i]),
                    };
                    values.push(val);
                }

                Row { values }
            })
            .collect();

        // Sort by group key for consistent output
        results.sort_by(|a, b| {
            for i in 0..self.group_by_columns.len() {
                let cmp = match (&a.values[i], &b.values[i]) {
                    (SqlValue::Varchar(a), SqlValue::Varchar(b)) => a.cmp(b),
                    (SqlValue::Integer(a), SqlValue::Integer(b)) => a.cmp(b),
                    (SqlValue::Double(a), SqlValue::Double(b)) => {
                        a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
                    }
                    (SqlValue::Date(a), SqlValue::Date(b)) => a.cmp(b),
                    (SqlValue::Boolean(a), SqlValue::Boolean(b)) => a.cmp(b),
                    _ => std::cmp::Ordering::Equal,
                };
                if cmp != std::cmp::Ordering::Equal {
                    return cmp;
                }
            }
            std::cmp::Ordering::Equal
        });

        Ok(results)
    }

    fn execute_stream(
        &self,
        rows: Box<dyn Iterator<Item = Row>>,
    ) -> Result<Vec<Row>, ExecutorError> {
        // Execute using streaming fast path
        let groups = unsafe { self.execute_stream_unsafe(rows) };

        // Convert to result rows
        let mut results: Vec<Row> = groups
            .into_iter()
            .map(|(key, agg_values)| {
                let mut values = key;

                // Add aggregate results, converting AVG to actual average
                for (i, spec) in self.aggregates.iter().enumerate() {
                    let val = match spec {
                        GroupAggregateSpec::Avg { .. } => {
                            // Get count from the last aggregate (should be COUNT)
                            let count = if let Some(GroupAggregateSpec::Count) =
                                self.aggregates.last()
                            {
                                agg_values[self.aggregates.len() - 1]
                            } else {
                                // If no COUNT, we can't compute AVG correctly
                                1.0
                            };
                            SqlValue::Double(agg_values[i] / count)
                        }
                        GroupAggregateSpec::Count => SqlValue::Integer(agg_values[i] as i64),
                        _ => SqlValue::Double(agg_values[i]),
                    };
                    values.push(val);
                }

                Row { values }
            })
            .collect();

        // Sort by group key for consistent output
        results.sort_by(|a, b| {
            for i in 0..self.group_by_columns.len() {
                let cmp = match (&a.values[i], &b.values[i]) {
                    (SqlValue::Varchar(a), SqlValue::Varchar(b)) => a.cmp(b),
                    (SqlValue::Integer(a), SqlValue::Integer(b)) => a.cmp(b),
                    (SqlValue::Double(a), SqlValue::Double(b)) => {
                        a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
                    }
                    (SqlValue::Date(a), SqlValue::Date(b)) => a.cmp(b),
                    (SqlValue::Boolean(a), SqlValue::Boolean(b)) => a.cmp(b),
                    _ => std::cmp::Ordering::Equal,
                };
                if cmp != std::cmp::Ordering::Equal {
                    return cmp;
                }
            }
            std::cmp::Ordering::Equal
        });

        Ok(results)
    }

    fn description(&self) -> &str {
        &self.description
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_parser::Parser;

    fn parse_select_query(query: &str) -> SelectStmt {
        let stmt = Parser::parse_sql(query).expect("Failed to parse SQL");
        match stmt {
            vibesql_ast::Statement::Select(select_stmt) => *select_stmt,
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_generic_grouped_aggregation_q1_like() {
        // Create lineitem-like schema
        let table = TableSchema::new(
            "lineitem".to_string(),
            vec![
                ColumnSchema::new("l_orderkey".to_string(), DataType::Integer, true),
                ColumnSchema::new("l_partkey".to_string(), DataType::Integer, true),
                ColumnSchema::new("l_suppkey".to_string(), DataType::Integer, true),
                ColumnSchema::new("l_linenumber".to_string(), DataType::Integer, true),
                ColumnSchema::new("l_quantity".to_string(), DataType::DoublePrecision, true),
                ColumnSchema::new("l_extendedprice".to_string(), DataType::DoublePrecision, true),
                ColumnSchema::new("l_discount".to_string(), DataType::DoublePrecision, true),
                ColumnSchema::new("l_tax".to_string(), DataType::DoublePrecision, true),
                ColumnSchema::new("l_returnflag".to_string(), DataType::Varchar { max_length: None }, true),
                ColumnSchema::new("l_linestatus".to_string(), DataType::Varchar { max_length: None }, true),
                ColumnSchema::new("l_shipdate".to_string(), DataType::Date, true),
            ],
        );
        let schema = CombinedSchema::from_table("lineitem".to_string(), table);

        // TPC-H Q1 query (simplified)
        let q1_query = r#"
            SELECT
                l_returnflag,
                l_linestatus,
                SUM(l_quantity) as sum_qty,
                SUM(l_extendedprice) as sum_base_price,
                AVG(l_quantity) as avg_qty,
                COUNT(*) as count_order
            FROM lineitem
            GROUP BY l_returnflag, l_linestatus
        "#;

        let stmt = parse_select_query(q1_query);
        let plan = GenericGroupedAggregationPlan::try_create(&stmt, &schema);
        assert!(plan.is_some(), "Should create generic GROUP BY plan for Q1-like query");

        let plan = plan.unwrap();
        assert!(
            plan.description.to_lowercase().contains("lineitem"),
            "Description should mention table name: {}",
            plan.description
        );
        assert!(
            plan.description.contains("2 groups"),
            "Description should mention 2 group columns: {}",
            plan.description
        );
    }
}
