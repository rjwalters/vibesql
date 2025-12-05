//! Aggregate-aware query optimization analysis
//!
//! This module analyzes GROUP BY and HAVING clauses to enable:
//! - Aggregate-aware join ordering (considering post-aggregate cardinality)
//! - Aggregate pushdown before joins (when beneficial)
//! - Early filtering on aggregate results

#![allow(clippy::redundant_closure, clippy::unnecessary_map_or)]

use std::collections::HashSet;
#[cfg(test)]
use vibesql_ast::GroupByClause;
use vibesql_ast::{Expression, SelectStmt};

/// Analysis of aggregate operations in a query
///
/// Identifies which tables are involved in aggregation and HAVING filters,
/// enabling aggregate-aware optimization decisions during query planning.
#[derive(Debug, Clone)]
pub struct AggregateAnalysis {
    /// Tables involved in GROUP BY expressions
    /// Example: `GROUP BY customer.region, customer.category`
    /// -> grouping_tables = {"customer"}
    pub grouping_tables: HashSet<String>,

    /// Tables involved in aggregate function arguments
    /// Example: `SUM(lineitem.quantity), COUNT(orders.orderkey)`
    /// -> aggregate_tables = {"lineitem", "orders"}
    pub aggregate_tables: HashSet<String>,

    /// Tables involved in HAVING predicate expressions
    /// Example: `HAVING SUM(lineitem.quantity) > 300 AND customer.region = 'ASIA'`
    /// -> having_tables = {"lineitem", "customer"}
    pub having_tables: HashSet<String>,

    /// Whether the query has aggregates (GROUP BY or aggregate functions)
    pub has_aggregates: bool,

    /// Whether the query has a HAVING clause
    pub has_having: bool,

    /// Aggregate pushdown opportunities identified during analysis
    pub pushdown_candidates: Vec<AggregatePushdown>,
}

/// A candidate for pushing aggregation down before joins
///
/// Represents an opportunity to compute aggregates on a table before joining,
/// potentially reducing intermediate result sizes significantly.
#[derive(Debug, Clone)]
pub struct AggregatePushdown {
    /// Table to aggregate before joining
    pub table: String,

    /// GROUP BY keys (must be join keys to preserve join correctness)
    pub group_keys: Vec<Expression>,

    /// Aggregate functions to compute early
    /// Example: SUM(quantity), COUNT(*)
    pub aggregates: Vec<AggregateInfo>,

    /// HAVING predicates that can filter early
    /// Only predicates that reference this table's aggregates
    pub filters: Vec<Expression>,

    /// Estimated selectivity of HAVING filter (0.0 to 1.0)
    /// Ratio of rows remaining after filter / rows before filter
    /// Lower values indicate more selective filters (better pushdown candidates)
    pub selectivity: f64,

    /// Estimated cardinality of table before aggregation
    pub cardinality_before: usize,

    /// Estimated cardinality after aggregation and HAVING filter
    pub cardinality_after: usize,
}

/// Information about an aggregate function in the query
#[derive(Debug, Clone)]
pub struct AggregateInfo {
    /// Aggregate function name (SUM, COUNT, AVG, MIN, MAX)
    pub function_name: String,

    /// Whether DISTINCT is specified
    pub distinct: bool,

    /// Arguments to the aggregate function
    pub args: Vec<Expression>,

    /// Tables referenced in the aggregate arguments
    pub referenced_tables: HashSet<String>,
}

impl AggregateAnalysis {
    /// Analyze a SELECT statement for aggregate optimization opportunities
    ///
    /// Identifies:
    /// - Which tables are involved in GROUP BY, aggregates, and HAVING
    /// - Whether aggregate pushdown is beneficial
    /// - Estimated selectivity of HAVING filters
    pub fn analyze(stmt: &SelectStmt) -> Self {
        let has_aggregates = stmt.group_by.is_some() || Self::has_aggregate_functions(stmt);
        let has_having = stmt.having.is_some();

        let mut grouping_tables = HashSet::new();
        let mut aggregate_tables = HashSet::new();
        let mut having_tables = HashSet::new();

        // Analyze GROUP BY expressions
        if let Some(ref group_by) = stmt.group_by {
            for expr in group_by.all_expressions() {
                Self::extract_table_refs(expr, &mut grouping_tables);
            }
        }

        // Analyze aggregate functions in SELECT list
        for select_item in &stmt.select_list {
            if let vibesql_ast::SelectItem::Expression { expr, .. } = select_item {
                Self::extract_aggregate_tables(expr, &mut aggregate_tables);
            }
        }

        // Analyze HAVING clause
        if let Some(ref having) = stmt.having {
            Self::extract_table_refs(having, &mut having_tables);
            Self::extract_aggregate_tables(having, &mut aggregate_tables);
        }

        // Identify pushdown candidates
        let pushdown_candidates = if has_aggregates && has_having {
            Self::identify_pushdown_candidates(stmt, &grouping_tables, &aggregate_tables)
        } else {
            Vec::new()
        };

        Self {
            grouping_tables,
            aggregate_tables,
            having_tables,
            has_aggregates,
            has_having,
            pushdown_candidates,
        }
    }

    /// Check if a SELECT statement contains any aggregate functions
    fn has_aggregate_functions(stmt: &SelectStmt) -> bool {
        // Check SELECT list
        for select_item in &stmt.select_list {
            if let vibesql_ast::SelectItem::Expression { expr, .. } = select_item {
                if Self::contains_aggregate(expr) {
                    return true;
                }
            }
        }

        // Check HAVING clause
        if let Some(ref having) = stmt.having {
            if Self::contains_aggregate(having) {
                return true;
            }
        }

        false
    }

    /// Check if an expression contains an aggregate function
    fn contains_aggregate(expr: &Expression) -> bool {
        match expr {
            Expression::AggregateFunction { .. } => true,
            Expression::BinaryOp { left, right, .. } => {
                Self::contains_aggregate(left) || Self::contains_aggregate(right)
            }
            Expression::UnaryOp { expr, .. } => Self::contains_aggregate(expr),
            Expression::Case { operand, when_clauses, else_result } => {
                if let Some(op) = operand {
                    if Self::contains_aggregate(op) {
                        return true;
                    }
                }
                for when_clause in when_clauses {
                    for condition in &when_clause.conditions {
                        if Self::contains_aggregate(condition) {
                            return true;
                        }
                    }
                    if Self::contains_aggregate(&when_clause.result) {
                        return true;
                    }
                }
                if let Some(else_expr) = else_result {
                    if Self::contains_aggregate(else_expr) {
                        return true;
                    }
                }
                false
            }
            Expression::Function { args, .. } => {
                args.iter().any(|arg| Self::contains_aggregate(arg))
            }
            Expression::IsNull { expr, .. } => Self::contains_aggregate(expr),
            Expression::In { expr, .. } => Self::contains_aggregate(expr),
            Expression::InList { expr, values, .. } => {
                Self::contains_aggregate(expr) || values.iter().any(|v| Self::contains_aggregate(v))
            }
            Expression::Between { expr, low, high, .. } => {
                Self::contains_aggregate(expr)
                    || Self::contains_aggregate(low)
                    || Self::contains_aggregate(high)
            }
            Expression::Cast { expr, .. } => Self::contains_aggregate(expr),
            Expression::Position { substring, string, .. } => {
                Self::contains_aggregate(substring) || Self::contains_aggregate(string)
            }
            Expression::Trim { removal_char, string, .. } => {
                removal_char.as_ref().map_or(false, |rc| Self::contains_aggregate(rc))
                    || Self::contains_aggregate(string)
            }
            Expression::Like { expr, pattern, .. } => {
                Self::contains_aggregate(expr) || Self::contains_aggregate(pattern)
            }
            _ => false,
        }
    }

    /// Extract table references from an expression
    fn extract_table_refs(expr: &Expression, tables: &mut HashSet<String>) {
        match expr {
            Expression::ColumnRef { table: Some(table), .. } => {
                tables.insert(table.clone());
            }
            Expression::BinaryOp { left, right, .. } => {
                Self::extract_table_refs(left, tables);
                Self::extract_table_refs(right, tables);
            }
            Expression::UnaryOp { expr, .. } => {
                Self::extract_table_refs(expr, tables);
            }
            Expression::Case { operand, when_clauses, else_result } => {
                if let Some(op) = operand {
                    Self::extract_table_refs(op, tables);
                }
                for when_clause in when_clauses {
                    for condition in &when_clause.conditions {
                        Self::extract_table_refs(condition, tables);
                    }
                    Self::extract_table_refs(&when_clause.result, tables);
                }
                if let Some(else_expr) = else_result {
                    Self::extract_table_refs(else_expr, tables);
                }
            }
            Expression::Function { args, .. } => {
                for arg in args {
                    Self::extract_table_refs(arg, tables);
                }
            }
            Expression::AggregateFunction { args, .. } => {
                for arg in args {
                    Self::extract_table_refs(arg, tables);
                }
            }
            Expression::IsNull { expr, .. } => {
                Self::extract_table_refs(expr, tables);
            }
            Expression::In { expr, .. } => {
                Self::extract_table_refs(expr, tables);
            }
            Expression::InList { expr, values, .. } => {
                Self::extract_table_refs(expr, tables);
                for value in values {
                    Self::extract_table_refs(value, tables);
                }
            }
            Expression::Between { expr, low, high, .. } => {
                Self::extract_table_refs(expr, tables);
                Self::extract_table_refs(low, tables);
                Self::extract_table_refs(high, tables);
            }
            Expression::Cast { expr, .. } => {
                Self::extract_table_refs(expr, tables);
            }
            Expression::Position { substring, string, .. } => {
                Self::extract_table_refs(substring, tables);
                Self::extract_table_refs(string, tables);
            }
            Expression::Trim { removal_char, string, .. } => {
                if let Some(rc) = removal_char {
                    Self::extract_table_refs(rc, tables);
                }
                Self::extract_table_refs(string, tables);
            }
            Expression::Like { expr, pattern, .. } => {
                Self::extract_table_refs(expr, tables);
                Self::extract_table_refs(pattern, tables);
            }
            _ => {}
        }
    }

    /// Extract tables referenced in aggregate function arguments
    fn extract_aggregate_tables(expr: &Expression, tables: &mut HashSet<String>) {
        match expr {
            Expression::AggregateFunction { args, .. } => {
                for arg in args {
                    Self::extract_table_refs(arg, tables);
                }
            }
            Expression::BinaryOp { left, right, .. } => {
                Self::extract_aggregate_tables(left, tables);
                Self::extract_aggregate_tables(right, tables);
            }
            Expression::UnaryOp { expr, .. } => {
                Self::extract_aggregate_tables(expr, tables);
            }
            Expression::Case { operand, when_clauses, else_result } => {
                if let Some(op) = operand {
                    Self::extract_aggregate_tables(op, tables);
                }
                for when_clause in when_clauses {
                    for condition in &when_clause.conditions {
                        Self::extract_aggregate_tables(condition, tables);
                    }
                    Self::extract_aggregate_tables(&when_clause.result, tables);
                }
                if let Some(else_expr) = else_result {
                    Self::extract_aggregate_tables(else_expr, tables);
                }
            }
            _ => {}
        }
    }

    /// Estimate the selectivity of a HAVING clause
    ///
    /// Returns estimated fraction of groups that pass the HAVING filter (0.0 to 1.0).
    /// Lower values indicate more selective filters.
    ///
    /// This is a heuristic-based estimation that doesn't require statistics.
    /// Uses pattern matching on common HAVING predicates to provide reasonable estimates.
    pub fn estimate_having_selectivity(having: &Expression) -> f64 {
        match having {
            // Comparisons with constants
            Expression::BinaryOp { op, left, right, .. } => {
                use vibesql_ast::BinaryOperator::*;

                // Handle logical operators first (they combine other predicates)
                match op {
                    And => {
                        let left_sel = Self::estimate_having_selectivity(left);
                        let right_sel = Self::estimate_having_selectivity(right);
                        left_sel * right_sel // Both conditions must pass
                    }
                    Or => {
                        let left_sel = Self::estimate_having_selectivity(left);
                        let right_sel = Self::estimate_having_selectivity(right);
                        left_sel + right_sel - (left_sel * right_sel) // Either passes
                    }
                    _ => {
                        // Check if comparing aggregate to constant
                        let has_aggregate =
                            Self::contains_aggregate(left) || Self::contains_aggregate(right);
                        let has_constant = matches!(left.as_ref(), Expression::Literal(_))
                            || matches!(right.as_ref(), Expression::Literal(_));

                        if has_aggregate && has_constant {
                            match op {
                                // Equality is typically very selective (1-5% of groups)
                                Equal => 0.05,

                                // Range comparisons moderately selective (10-30%)
                                GreaterThan | GreaterThanOrEqual | LessThan | LessThanOrEqual => {
                                    0.25
                                }

                                // Inequality less selective (50-95%)
                                NotEqual => 0.75,

                                _ => 0.5, // Unknown operator, assume 50%
                            }
                        } else {
                            // Non-aggregate or non-constant comparison
                            0.5
                        }
                    }
                }
            }

            // BETWEEN is moderately selective
            Expression::Between { expr, .. } => {
                if Self::contains_aggregate(expr) {
                    0.3 // 30% of groups in range
                } else {
                    0.5
                }
            }

            // IN list selectivity depends on list size
            Expression::In { expr, .. } => {
                if Self::contains_aggregate(expr) {
                    0.2 // Subquery IN, assume 20%
                } else {
                    0.5
                }
            }
            Expression::InList { expr, values, .. } => {
                if Self::contains_aggregate(expr) {
                    // Selectivity proportional to list size, but capped
                    (values.len() as f64 * 0.05).min(0.5)
                } else {
                    0.5
                }
            }

            // NOT inverts selectivity
            Expression::UnaryOp { op: vibesql_ast::UnaryOperator::Not, expr } => {
                1.0 - Self::estimate_having_selectivity(expr)
            }

            // IS NULL / IS NOT NULL
            Expression::IsNull { expr, negated } => {
                if Self::contains_aggregate(expr) {
                    if *negated {
                        0.95 // Most aggregates are NOT NULL
                    } else {
                        0.05 // Few aggregates are NULL
                    }
                } else {
                    0.1
                }
            }

            // Default: assume 50% selectivity (no information)
            _ => 0.5,
        }
    }

    /// Identify opportunities to push aggregation down before joins
    ///
    /// Conditions for pushdown:
    /// 1. GROUP BY keys are join keys (to preserve join semantics)
    /// 2. HAVING is selective (estimated selectivity < 0.1)
    /// 3. Table is large (> 100K rows estimated)
    /// 4. No complex correlations preventing independence
    fn identify_pushdown_candidates(
        _stmt: &SelectStmt,
        _grouping_tables: &HashSet<String>,
        _aggregate_tables: &HashSet<String>,
    ) -> Vec<AggregatePushdown> {
        // TODO: Implement pushdown candidate identification in Milestone 3
        // This requires:
        // 1. Join graph analysis to identify join keys
        // 2. Checking if GROUP BY keys match join keys
        // 3. HAVING selectivity estimation (now implemented above)
        // 4. Table cardinality estimation from statistics
        Vec::new()
    }
}

impl AggregatePushdown {
    /// Estimate the benefit of pushing down this aggregate
    ///
    /// Returns a score indicating how beneficial this pushdown would be.
    /// Higher scores indicate better optimization opportunities.
    ///
    /// Score calculation:
    /// - Reduction ratio = cardinality_before / cardinality_after
    /// - Benefit = reduction_ratio * (1.0 - selectivity)
    pub fn estimate_benefit(&self) -> f64 {
        if self.cardinality_after == 0 {
            return 0.0;
        }

        let reduction_ratio = self.cardinality_before as f64 / self.cardinality_after as f64;
        reduction_ratio * (1.0 - self.selectivity)
    }

    /// Check if this pushdown is beneficial
    ///
    /// Heuristic: Pushdown is beneficial if it reduces cardinality by at least 10x
    pub fn is_beneficial(&self) -> bool {
        self.cardinality_before / self.cardinality_after.max(1) >= 10
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_ast::{Expression, SelectItem, SelectStmt};

    fn make_column_ref(table: &str, column: &str) -> Expression {
        Expression::ColumnRef { table: Some(table.to_string()), column: column.to_string() }
    }

    fn make_aggregate(name: &str, arg: Expression) -> Expression {
        Expression::AggregateFunction { name: name.to_string(), distinct: false, args: vec![arg] }
    }

    #[test]
    fn test_no_aggregates() {
        let stmt = SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: vec![SelectItem::Expression {
                expr: make_column_ref("users", "name"),
                alias: None,
            }],
            into_table: None,
            into_variables: None,
            from: None,
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
        };

        let analysis = AggregateAnalysis::analyze(&stmt);
        assert!(!analysis.has_aggregates);
        assert!(!analysis.has_having);
        assert!(analysis.grouping_tables.is_empty());
        assert!(analysis.aggregate_tables.is_empty());
    }

    #[test]
    fn test_simple_group_by() {
        let stmt = SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: vec![
                SelectItem::Expression {
                    expr: make_column_ref("orders", "customer_id"),
                    alias: None,
                },
                SelectItem::Expression {
                    expr: make_aggregate("COUNT", Expression::Wildcard),
                    alias: Some("order_count".to_string()),
                },
            ],
            into_table: None,
            into_variables: None,
            from: None,
            where_clause: None,
            group_by: Some(GroupByClause::Simple(vec![make_column_ref("orders", "customer_id")])),
            having: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
        };

        let analysis = AggregateAnalysis::analyze(&stmt);
        assert!(analysis.has_aggregates);
        assert!(!analysis.has_having);
        assert!(analysis.grouping_tables.contains("orders"));
        assert!(!analysis.aggregate_tables.contains("orders")); // COUNT(*) doesn't reference specific table
    }

    #[test]
    fn test_having_clause() {
        let stmt = SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: vec![
                SelectItem::Expression {
                    expr: make_column_ref("lineitem", "l_orderkey"),
                    alias: None,
                },
                SelectItem::Expression {
                    expr: make_aggregate("SUM", make_column_ref("lineitem", "l_quantity")),
                    alias: Some("total_qty".to_string()),
                },
            ],
            into_table: None,
            into_variables: None,
            from: None,
            where_clause: None,
            group_by: Some(GroupByClause::Simple(vec![make_column_ref("lineitem", "l_orderkey")])),
            having: Some(Expression::BinaryOp {
                op: vibesql_ast::BinaryOperator::GreaterThan,
                left: Box::new(make_aggregate("SUM", make_column_ref("lineitem", "l_quantity"))),
                right: Box::new(Expression::Literal(vibesql_types::SqlValue::Integer(300))),
            }),
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
        };

        let analysis = AggregateAnalysis::analyze(&stmt);
        assert!(analysis.has_aggregates);
        assert!(analysis.has_having);
        assert!(analysis.grouping_tables.contains("lineitem"));
        assert!(analysis.aggregate_tables.contains("lineitem"));
        assert!(analysis.having_tables.contains("lineitem"));
    }

    #[test]
    fn test_aggregate_benefit_estimation() {
        let pushdown = AggregatePushdown {
            table: "lineitem".to_string(),
            group_keys: vec![],
            aggregates: vec![],
            filters: vec![],
            selectivity: 0.05, // 5% of groups pass HAVING
            cardinality_before: 6_000_000,
            cardinality_after: 60,
        };

        assert!(pushdown.is_beneficial()); // 100,000x reduction
        assert!(pushdown.estimate_benefit() > 0.0);
    }

    #[test]
    fn test_having_selectivity_equality() {
        // HAVING SUM(quantity) = 300 (very selective)
        let having = Expression::BinaryOp {
            op: vibesql_ast::BinaryOperator::Equal,
            left: Box::new(make_aggregate("SUM", make_column_ref("lineitem", "quantity"))),
            right: Box::new(Expression::Literal(vibesql_types::SqlValue::Integer(300))),
        };

        let selectivity = AggregateAnalysis::estimate_having_selectivity(&having);
        assert_eq!(selectivity, 0.05); // 5% for equality
    }

    #[test]
    fn test_having_selectivity_range() {
        // HAVING SUM(quantity) > 300 (moderately selective)
        let having = Expression::BinaryOp {
            op: vibesql_ast::BinaryOperator::GreaterThan,
            left: Box::new(make_aggregate("SUM", make_column_ref("lineitem", "quantity"))),
            right: Box::new(Expression::Literal(vibesql_types::SqlValue::Integer(300))),
        };

        let selectivity = AggregateAnalysis::estimate_having_selectivity(&having);
        assert_eq!(selectivity, 0.25); // 25% for range comparison
    }

    #[test]
    fn test_having_selectivity_and() {
        // HAVING SUM(quantity) > 300 AND COUNT(*) > 5
        // Combined selectivity: 0.25 * 0.25 = 0.0625
        let having = Expression::BinaryOp {
            op: vibesql_ast::BinaryOperator::And,
            left: Box::new(Expression::BinaryOp {
                op: vibesql_ast::BinaryOperator::GreaterThan,
                left: Box::new(make_aggregate("SUM", make_column_ref("lineitem", "quantity"))),
                right: Box::new(Expression::Literal(vibesql_types::SqlValue::Integer(300))),
            }),
            right: Box::new(Expression::BinaryOp {
                op: vibesql_ast::BinaryOperator::GreaterThan,
                left: Box::new(make_aggregate("COUNT", Expression::Wildcard)),
                right: Box::new(Expression::Literal(vibesql_types::SqlValue::Integer(5))),
            }),
        };

        let selectivity = AggregateAnalysis::estimate_having_selectivity(&having);
        assert_eq!(selectivity, 0.0625); // 25% * 25%
    }

    #[test]
    fn test_having_selectivity_or() {
        // HAVING SUM(quantity) > 300 OR SUM(quantity) < 50
        // Combined selectivity: 0.25 + 0.25 - (0.25 * 0.25) = 0.4375
        let having = Expression::BinaryOp {
            op: vibesql_ast::BinaryOperator::Or,
            left: Box::new(Expression::BinaryOp {
                op: vibesql_ast::BinaryOperator::GreaterThan,
                left: Box::new(make_aggregate("SUM", make_column_ref("lineitem", "quantity"))),
                right: Box::new(Expression::Literal(vibesql_types::SqlValue::Integer(300))),
            }),
            right: Box::new(Expression::BinaryOp {
                op: vibesql_ast::BinaryOperator::LessThan,
                left: Box::new(make_aggregate("SUM", make_column_ref("lineitem", "quantity"))),
                right: Box::new(Expression::Literal(vibesql_types::SqlValue::Integer(50))),
            }),
        };

        let selectivity = AggregateAnalysis::estimate_having_selectivity(&having);
        assert_eq!(selectivity, 0.4375); // 25% + 25% - (25% * 25%)
    }

    #[test]
    fn test_having_selectivity_is_null() {
        // HAVING SUM(quantity) IS NULL (rare for aggregates)
        let having = Expression::IsNull {
            expr: Box::new(make_aggregate("SUM", make_column_ref("lineitem", "quantity"))),
            negated: false,
        };

        let selectivity = AggregateAnalysis::estimate_having_selectivity(&having);
        assert_eq!(selectivity, 0.05); // 5% for IS NULL on aggregates
    }

    #[test]
    fn test_having_selectivity_is_not_null() {
        // HAVING SUM(quantity) IS NOT NULL (common for aggregates)
        let having = Expression::IsNull {
            expr: Box::new(make_aggregate("SUM", make_column_ref("lineitem", "quantity"))),
            negated: true,
        };

        let selectivity = AggregateAnalysis::estimate_having_selectivity(&having);
        assert_eq!(selectivity, 0.95); // 95% for IS NOT NULL on aggregates
    }
}
