//! Adaptive execution strategy selection
//!
//! This module provides a clean, testable abstraction for selecting the optimal
//! execution strategy for a given query. It uses zero-cost trait-based policies
//! to maintain performance while improving code organization.
//!
//! ## Architecture
//!
//! - `QueryProfile`: Analyzes query characteristics once
//! - `ExecutionStrategy`: Enum of available execution paths
//! - `ExecutionPolicy`: Trait for strategy selection (zero-cost via monomorphization)
//! - `DefaultExecutionPolicy`: Current heuristics extracted into policy

use vibesql_ast::{Expression, FromClause, SelectItem, SelectStmt};

/// Available execution strategies
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionStrategy {
    /// SIMD-accelerated columnar processing
    Columnar,
    /// Traditional row-at-a-time execution
    RowBased,
}

/// Complexity level of query predicates
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PredicateComplexity {
    /// No WHERE clause
    None,
    /// Simple comparisons (=, <, >, <=, >=) and AND combinations
    Simple,
    /// Complex predicates (OR, IN, LIKE, subqueries)
    Complex,
}

/// Query characteristics extracted once for policy evaluation
#[derive(Debug, Clone)]
pub struct QueryProfile {
    pub has_aggregates: bool,
    pub has_group_by: bool,
    pub has_joins: bool,
    pub has_window_functions: bool,
    pub has_distinct: bool,
    pub predicate_complexity: PredicateComplexity,
}

impl QueryProfile {
    /// Extract query profile from SELECT statement
    pub fn from_stmt(stmt: &SelectStmt) -> Self {
        Self {
            has_aggregates: Self::detect_aggregates(&stmt.select_list),
            has_group_by: stmt.group_by.is_some(),
            has_joins: Self::detect_joins(stmt.from.as_ref()),
            has_window_functions: Self::detect_window_functions(&stmt.select_list),
            has_distinct: stmt.distinct,
            predicate_complexity: Self::analyze_predicate(stmt.where_clause.as_ref()),
        }
    }

    /// Check if SELECT list contains aggregate functions
    fn detect_aggregates(select_list: &[SelectItem]) -> bool {
        select_list.iter().any(|item| {
            if let SelectItem::Expression { expr, .. } = item {
                Self::has_aggregate_in_expr(expr)
            } else {
                false
            }
        })
    }

    /// Recursively check if expression contains aggregate functions
    fn has_aggregate_in_expr(expr: &Expression) -> bool {
        match expr {
            Expression::AggregateFunction { .. } => true,
            Expression::BinaryOp { left, right, .. } => {
                Self::has_aggregate_in_expr(left) || Self::has_aggregate_in_expr(right)
            }
            Expression::UnaryOp { expr, .. } => Self::has_aggregate_in_expr(expr),
            Expression::Function { args, .. } => {
                args.iter().any(|arg| Self::has_aggregate_in_expr(arg))
            }
            Expression::Case { when_clauses, else_result, .. } => {
                when_clauses.iter().any(|clause| {
                    clause.conditions.iter().any(|c| Self::has_aggregate_in_expr(c))
                        || Self::has_aggregate_in_expr(&clause.result)
                }) || else_result.as_ref().map_or(false, |e| Self::has_aggregate_in_expr(e))
            }
            _ => false,
        }
    }

    /// Check if FROM clause contains JOINs
    fn detect_joins(from: Option<&FromClause>) -> bool {
        match from {
            Some(FromClause::Join { .. }) => true,
            _ => false,
        }
    }

    /// Check if SELECT list contains window functions
    fn detect_window_functions(select_list: &[SelectItem]) -> bool {
        select_list.iter().any(|item| {
            if let SelectItem::Expression { expr, .. } = item {
                Self::has_window_function_in_expr(expr)
            } else {
                false
            }
        })
    }

    /// Recursively check if expression contains window functions
    fn has_window_function_in_expr(expr: &Expression) -> bool {
        match expr {
            Expression::WindowFunction { .. } => true,
            Expression::BinaryOp { left, right, .. } => {
                Self::has_window_function_in_expr(left) || Self::has_window_function_in_expr(right)
            }
            Expression::UnaryOp { expr, .. } => Self::has_window_function_in_expr(expr),
            Expression::Function { args, .. } => {
                args.iter().any(|arg| Self::has_window_function_in_expr(arg))
            }
            _ => false,
        }
    }

    /// Analyze WHERE clause predicate complexity
    fn analyze_predicate(where_clause: Option<&Expression>) -> PredicateComplexity {
        match where_clause {
            None => PredicateComplexity::None,
            Some(expr) => {
                if Self::is_simple_predicate(expr) {
                    PredicateComplexity::Simple
                } else {
                    PredicateComplexity::Complex
                }
            }
        }
    }

    /// Check if expression is a simple predicate suitable for columnar execution
    fn is_simple_predicate(expr: &Expression) -> bool {
        use vibesql_ast::BinaryOperator;

        match expr {
            // Simple binary comparisons
            Expression::BinaryOp { op, left, right } => match op {
                BinaryOperator::Equal
                | BinaryOperator::NotEqual
                | BinaryOperator::LessThan
                | BinaryOperator::LessThanOrEqual
                | BinaryOperator::GreaterThan
                | BinaryOperator::GreaterThanOrEqual => true,
                // AND is simple if both sides are simple
                BinaryOperator::And => {
                    Self::is_simple_predicate(left) && Self::is_simple_predicate(right)
                }
                // Everything else is complex
                _ => false,
            },
            // BETWEEN is simple
            Expression::Between { .. } => true,
            // Everything else is complex
            _ => false,
        }
    }
}

/// Policy trait for execution strategy selection (zero-cost via monomorphization)
pub trait ExecutionPolicy {
    fn select_strategy(&self, profile: &QueryProfile) -> ExecutionStrategy;
}

/// Default execution policy matching current behavior
#[derive(Debug, Clone, Copy)]
pub struct DefaultExecutionPolicy;

impl ExecutionPolicy for DefaultExecutionPolicy {
    fn select_strategy(&self, profile: &QueryProfile) -> ExecutionStrategy {
        if can_use_columnar(profile) {
            ExecutionStrategy::Columnar
        } else {
            ExecutionStrategy::RowBased
        }
    }
}

/// Determine if columnar execution is suitable for this query
///
/// Columnar execution is beneficial for queries that:
/// 1. Have aggregations (SUM, AVG, MIN, MAX, COUNT)
/// 2. Have simple predicates (=, <, >, <=, >=, BETWEEN, AND combinations)
/// 3. Scan a single table (no JOINs for now)
/// 4. Don't use complex features (window functions, DISTINCT, GROUP BY)
pub fn can_use_columnar(profile: &QueryProfile) -> bool {
    // Must have aggregates
    if !profile.has_aggregates {
        return false;
    }

    // No GROUP BY support yet (Phase 5 limitation)
    // TODO: Add GROUP BY support in future phase
    if profile.has_group_by {
        return false;
    }

    // No JOIN support yet
    // TODO: Add JOIN support in future phase
    if profile.has_joins {
        return false;
    }

    // No window functions
    if profile.has_window_functions {
        return false;
    }

    // No DISTINCT for now
    if profile.has_distinct {
        return false;
    }

    // Only simple predicates
    if profile.predicate_complexity == PredicateComplexity::Complex {
        return false;
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_aggregate_query_uses_columnar() {
        // SELECT SUM(x) FROM t WHERE y > 10
        let profile = QueryProfile {
            has_aggregates: true,
            has_group_by: false,
            has_joins: false,
            has_window_functions: false,
            has_distinct: false,
            predicate_complexity: PredicateComplexity::Simple,
        };

        let policy = DefaultExecutionPolicy;
        assert_eq!(policy.select_strategy(&profile), ExecutionStrategy::Columnar);
    }

    #[test]
    fn test_group_by_uses_row_based() {
        // SELECT SUM(x) FROM t GROUP BY y
        let profile = QueryProfile {
            has_aggregates: true,
            has_group_by: true,
            has_joins: false,
            has_window_functions: false,
            has_distinct: false,
            predicate_complexity: PredicateComplexity::None,
        };

        let policy = DefaultExecutionPolicy;
        assert_eq!(policy.select_strategy(&profile), ExecutionStrategy::RowBased);
    }

    #[test]
    fn test_join_uses_row_based() {
        // SELECT SUM(a.x) FROM a JOIN b ON a.id = b.id
        let profile = QueryProfile {
            has_aggregates: true,
            has_group_by: false,
            has_joins: true,
            has_window_functions: false,
            has_distinct: false,
            predicate_complexity: PredicateComplexity::None,
        };

        let policy = DefaultExecutionPolicy;
        assert_eq!(policy.select_strategy(&profile), ExecutionStrategy::RowBased);
    }

    #[test]
    fn test_window_function_uses_row_based() {
        // SELECT ROW_NUMBER() OVER (ORDER BY x) FROM t
        let profile = QueryProfile {
            has_aggregates: false,
            has_group_by: false,
            has_joins: false,
            has_window_functions: true,
            has_distinct: false,
            predicate_complexity: PredicateComplexity::None,
        };

        let policy = DefaultExecutionPolicy;
        assert_eq!(policy.select_strategy(&profile), ExecutionStrategy::RowBased);
    }

    #[test]
    fn test_complex_predicate_uses_row_based() {
        // SELECT SUM(x) FROM t WHERE y IN (1, 2, 3)
        let profile = QueryProfile {
            has_aggregates: true,
            has_group_by: false,
            has_joins: false,
            has_window_functions: false,
            has_distinct: false,
            predicate_complexity: PredicateComplexity::Complex,
        };

        let policy = DefaultExecutionPolicy;
        assert_eq!(policy.select_strategy(&profile), ExecutionStrategy::RowBased);
    }

    #[test]
    fn test_distinct_uses_row_based() {
        // SELECT DISTINCT SUM(x) FROM t
        let profile = QueryProfile {
            has_aggregates: true,
            has_group_by: false,
            has_joins: false,
            has_window_functions: false,
            has_distinct: true,
            predicate_complexity: PredicateComplexity::None,
        };

        let policy = DefaultExecutionPolicy;
        assert_eq!(policy.select_strategy(&profile), ExecutionStrategy::RowBased);
    }

    #[test]
    fn test_no_aggregates_uses_row_based() {
        // SELECT * FROM t WHERE x > 10
        let profile = QueryProfile {
            has_aggregates: false,
            has_group_by: false,
            has_joins: false,
            has_window_functions: false,
            has_distinct: false,
            predicate_complexity: PredicateComplexity::Simple,
        };

        let policy = DefaultExecutionPolicy;
        assert_eq!(policy.select_strategy(&profile), ExecutionStrategy::RowBased);
    }
}
