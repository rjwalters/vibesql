//! Generic single-table aggregation patterns
//!
//! This module provides generic monomorphic execution plans for single-table
//! aggregation queries (e.g., SUM, COUNT without GROUP BY).

use vibesql_ast::{BinaryOperator, Expression, SelectStmt};
use vibesql_storage::Row;
use vibesql_types::SqlValue;

use crate::{errors::ExecutorError, schema::CombinedSchema};

use super::super::pattern::{count_aggregate_functions, has_aggregate_function, has_no_joins, QueryPattern};
use super::super::MonomorphicPlan;
use super::filter::{extract_filters, optimize_date_ranges, FilterPredicate};

/// Specification for an aggregation operation
#[derive(Debug, Clone)]
pub enum AggregationSpec {
    /// SUM(col1 * col2) - multiply two columns and sum
    SumProduct {
        col1_idx: usize,
        col2_idx: usize,
    },
    /// SUM(col) - simple sum
    Sum {
        col_idx: usize,
    },
    /// COUNT(*) or COUNT(col)
    Count,
}

impl AggregationSpec {
    /// Get the initial accumulator value
    pub fn initial_value(&self) -> f64 {
        match self {
            AggregationSpec::SumProduct { .. } | AggregationSpec::Sum { .. } => 0.0,
            AggregationSpec::Count => 0.0,
        }
    }

    /// Accumulate a value
    ///
    /// # Safety
    ///
    /// Uses unchecked accessors for performance. Safe because column indices
    /// are validated at plan creation time.
    #[inline(always)]
    pub unsafe fn accumulate(&self, acc: f64, row: &Row) -> f64 {
        match self {
            AggregationSpec::SumProduct { col1_idx, col2_idx } => {
                let val1 = row.get_numeric_as_f64_unchecked(*col1_idx);
                let val2 = row.get_numeric_as_f64_unchecked(*col2_idx);
                acc + (val1 * val2)
            }
            AggregationSpec::Sum { col_idx } => {
                let val = row.get_numeric_as_f64_unchecked(*col_idx);
                acc + val
            }
            AggregationSpec::Count => acc + 1.0,
        }
    }
}

/// Generic filtered aggregation plan
///
/// Handles queries like:
/// ```sql
/// SELECT SUM(col1 * col2)
/// FROM any_table
/// WHERE <filters>
/// ```
///
/// This plan is generic - it works for any table and any compatible filters.
pub struct GenericFilteredAggregationPlan {
    /// Filter predicates to evaluate
    filters: Vec<FilterPredicate>,

    /// Aggregation to compute
    aggregation: AggregationSpec,

    /// Description for debugging
    #[allow(dead_code)]
    description: String,
}

impl GenericFilteredAggregationPlan {
    /// Try to create a generic filtered aggregation plan
    pub fn try_create(stmt: &SelectStmt, schema: &CombinedSchema) -> Option<Self> {
        // Must have no joins
        if !has_no_joins(&stmt.from) {
            return None;
        }

        // Must have no GROUP BY or HAVING
        if stmt.group_by.is_some() || stmt.having.is_some() {
            return None;
        }

        // Must have SUM aggregate
        if !has_aggregate_function(&stmt.select_list, "SUM") {
            return None;
        }

        // Must have exactly ONE aggregate function (this plan only supports single-aggregate queries)
        let aggregate_count = count_aggregate_functions(&stmt.select_list);
        if aggregate_count != 1 {
            return None;
        }

        // Extract aggregation specification
        let aggregation = Self::extract_aggregation(&stmt.select_list, schema)?;

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
                "Generic Filtered Aggregation on {} ({} filters)",
                table_name,
                optimized_filters.len()
            ),
            filters: optimized_filters,
            aggregation,
        })
    }

    /// Extract aggregation specification from SELECT list
    fn extract_aggregation(
        select_list: &[vibesql_ast::SelectItem],
        schema: &CombinedSchema,
    ) -> Option<AggregationSpec> {
        for item in select_list {
            if let vibesql_ast::SelectItem::Expression { expr, .. } = item {
                if let Some(spec) = Self::extract_aggregation_from_expr(expr, schema) {
                    return Some(spec);
                }
            }
        }
        None
    }

    /// Extract aggregation from an expression
    fn extract_aggregation_from_expr(
        expr: &Expression,
        schema: &CombinedSchema,
    ) -> Option<AggregationSpec> {
        match expr {
            Expression::AggregateFunction { name, args, .. } => {
                if name.eq_ignore_ascii_case("SUM") && args.len() == 1 {
                    // Check for SUM(col1 * col2)
                    if let Expression::BinaryOp {
                        op: BinaryOperator::Multiply,
                        left,
                        right,
                    } = &args[0]
                    {
                        let col1 = match left.as_ref() {
                            Expression::ColumnRef { column, .. } => column,
                            _ => return None,
                        };
                        let col2 = match right.as_ref() {
                            Expression::ColumnRef { column, .. } => column,
                            _ => return None,
                        };

                        let col1_idx = schema.get_column_index(None, col1)?;
                        let col2_idx = schema.get_column_index(None, col2)?;

                        return Some(AggregationSpec::SumProduct { col1_idx, col2_idx });
                    }

                    // Check for SUM(col)
                    if let Expression::ColumnRef { column, .. } = &args[0] {
                        let col_idx = schema.get_column_index(None, column)?;
                        return Some(AggregationSpec::Sum { col_idx });
                    }
                } else if name.eq_ignore_ascii_case("COUNT") {
                    return Some(AggregationSpec::Count);
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
    unsafe fn execute_unsafe(&self, rows: &[Row]) -> f64 {
        let mut accumulator = self.aggregation.initial_value();

        for row in rows {
            // Evaluate all filters
            let mut pass = true;
            for filter in &self.filters {
                if !filter.evaluate(row) {
                    pass = false;
                    break;
                }
            }

            // If all filters pass, accumulate
            if pass {
                accumulator = self.aggregation.accumulate(accumulator, row);
            }
        }

        accumulator
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
    unsafe fn execute_stream_unsafe(&self, rows: Box<dyn Iterator<Item = Row>>) -> f64 {
        let mut accumulator = self.aggregation.initial_value();

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

            // If all filters pass, accumulate
            if pass {
                accumulator = self.aggregation.accumulate(accumulator, &row);
            }
        }

        accumulator
    }
}

impl MonomorphicPlan for GenericFilteredAggregationPlan {
    fn execute(&self, rows: &[Row]) -> Result<Vec<Row>, ExecutorError> {
        // Execute using unsafe fast path
        let result = unsafe { self.execute_unsafe(rows) };

        // Return single-row result
        Ok(vec![Row {
            values: vec![SqlValue::Double(result)],
        }])
    }

    fn execute_stream(
        &self,
        rows: Box<dyn Iterator<Item = Row>>,
    ) -> Result<Vec<Row>, ExecutorError> {
        // Execute using streaming fast path
        let result = unsafe { self.execute_stream_unsafe(rows) };

        // Return single-row result
        Ok(vec![Row {
            values: vec![SqlValue::Double(result)],
        }])
    }

    fn description(&self) -> &str {
        &self.description
    }
}

/// Pattern matcher for generic filtered aggregation queries
#[allow(dead_code)]
pub struct GenericFilteredAggregationMatcher;

impl QueryPattern for GenericFilteredAggregationMatcher {
    fn matches(&self, stmt: &SelectStmt, schema: &CombinedSchema) -> bool {
        // Try to create the plan - if successful, it matches
        GenericFilteredAggregationPlan::try_create(stmt, schema).is_some()
    }

    fn description(&self) -> &str {
        "Generic Filtered Aggregation Pattern"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_parser::Parser;
    use vibesql_types::DataType;

    fn parse_select_query(query: &str) -> SelectStmt {
        let stmt = Parser::parse_sql(query).expect("Failed to parse SQL");
        match stmt {
            vibesql_ast::Statement::Select(select_stmt) => *select_stmt,
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_generic_pattern_q6_like() {
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

        // TPC-H Q6 query (note: using bare strings without DATE keyword for compatibility)
        let q6_query = r#"
            SELECT SUM(l_extendedprice * l_discount) as revenue
            FROM lineitem
            WHERE
                l_shipdate >= '1994-01-01'
                AND l_shipdate < '1995-01-01'
                AND l_discount BETWEEN 0.05 AND 0.07
                AND l_quantity < 24
        "#;

        let stmt = parse_select_query(q6_query);
        let plan = GenericFilteredAggregationPlan::try_create(&stmt, &schema);
        assert!(plan.is_some(), "Should create generic plan for Q6-like query");

        let plan = plan.unwrap();
        assert!(
            plan.description.to_lowercase().contains("lineitem"),
            "Description should mention table name"
        );
    }

    #[test]
    fn test_generic_pattern_different_table() {
        // Create a sales table with similar structure
        let table = TableSchema::new(
            "sales".to_string(),
            vec![
                ColumnSchema::new("sale_date".to_string(), DataType::Date, true),
                ColumnSchema::new("price".to_string(), DataType::DoublePrecision, true),
                ColumnSchema::new("discount".to_string(), DataType::DoublePrecision, true),
                ColumnSchema::new("quantity".to_string(), DataType::DoublePrecision, true),
            ],
        );
        let schema = CombinedSchema::from_table("sales".to_string(), table);

        // Similar query on different table (note: using bare strings without DATE keyword)
        let query = r#"
            SELECT SUM(price * discount) as revenue
            FROM sales
            WHERE
                sale_date >= '2024-01-01'
                AND sale_date < '2024-02-01'
                AND discount BETWEEN 0.10 AND 0.20
                AND quantity < 50
        "#;

        let stmt = parse_select_query(query);
        let plan = GenericFilteredAggregationPlan::try_create(&stmt, &schema);
        assert!(
            plan.is_some(),
            "Should create generic plan for different table"
        );

        let plan = plan.unwrap();
        assert!(
            plan.description.to_lowercase().contains("sales"),
            "Description should mention sales table"
        );
    }
}
