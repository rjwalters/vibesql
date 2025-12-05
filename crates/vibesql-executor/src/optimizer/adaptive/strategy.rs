//! Unified Execution Strategy Selection
//!
//! This module provides a comprehensive `ExecutionStrategy` enum that encapsulates
//! all execution path selection logic in a single decision point. This replaces the
//! previous scattered eligibility checks across multiple execution methods.
//!
//! ## Execution Strategies
//!
//! - **NativeColumnar**: Zero-copy SIMD execution directly from columnar storage
//! - **StandardColumnar**: SIMD execution with row-to-batch conversion
//! - **RowOriented**: Traditional row-by-row execution (with or without aggregation)
//! - **ExpressionOnly**: SELECT without FROM clause
//!
//! ## Benefits
//!
//! - Single decision point for observability and debugging
//! - Clear reasoning captured in `StrategyScore`
//! - Extensible for future strategies (JIT, GPU, distributed)
//! - Preserves existing hint mechanism

use std::collections::HashMap;

use vibesql_ast::{FromClause, SelectStmt};

use super::patterns::has_analytical_pattern;
use super::query::{has_aggregate_functions, has_group_by, is_single_table};
use crate::select::cte::CteResult;

/// Comprehensive execution strategy selection
///
/// Each variant represents a distinct execution path with its selection rationale.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionStrategy {
    /// Zero-copy SIMD execution directly from columnar storage (fastest for eligible queries)
    ///
    /// Requirements:
    /// - Single table scan (no JOINs)
    /// - No CTEs or set operations
    /// - Analytical pattern detected
    /// - native-columnar feature enabled or VIBESQL_NATIVE_COLUMNAR env var set
    NativeColumnar {
        /// Table name for columnar access
        table: String,
        /// Explains why this strategy was chosen
        score: StrategyScore,
    },

    /// SIMD execution with row-to-batch conversion
    ///
    /// Requirements:
    /// - No GROUP BY (handled by NativeColumnar path)
    /// - No CTEs or set operations
    /// - Has FROM clause
    /// - Analytical pattern detected
    StandardColumnar {
        /// Explains why this strategy was chosen
        score: StrategyScore,
    },

    /// Traditional row-oriented execution
    ///
    /// Used when:
    /// - Query doesn't benefit from columnar execution
    /// - Has features not yet supported by columnar paths
    /// - Explicit ROW_ORIENTED hint
    RowOriented {
        /// Whether the query has aggregation
        has_aggregation: bool,
        /// Whether the query has GROUP BY
        has_group_by: bool,
        /// Explains why this strategy was chosen
        score: StrategyScore,
    },

    /// SELECT without FROM clause (expression evaluation only)
    ///
    /// For queries like: SELECT 1 + 1, CURRENT_DATE
    ExpressionOnly {
        /// Explains why this strategy was chosen
        score: StrategyScore,
    },
}

impl ExecutionStrategy {
    /// Get the strategy score for debugging/observability
    pub fn score(&self) -> &StrategyScore {
        match self {
            ExecutionStrategy::NativeColumnar { score, .. } => score,
            ExecutionStrategy::StandardColumnar { score } => score,
            ExecutionStrategy::RowOriented { score, .. } => score,
            ExecutionStrategy::ExpressionOnly { score } => score,
        }
    }

    /// Get a human-readable name for the strategy
    pub fn name(&self) -> &'static str {
        match self {
            ExecutionStrategy::NativeColumnar { .. } => "NativeColumnar",
            ExecutionStrategy::StandardColumnar { .. } => "StandardColumnar",
            ExecutionStrategy::RowOriented { .. } => "RowOriented",
            ExecutionStrategy::ExpressionOnly { .. } => "ExpressionOnly",
        }
    }
}

/// Explains why a strategy was chosen
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrategyScore {
    /// Primary reason for the strategy selection
    pub reason: StrategyReason,
    /// Additional context for debugging
    pub details: Option<String>,
}

impl StrategyScore {
    /// Create a new strategy score
    pub fn new(reason: StrategyReason) -> Self {
        Self { reason, details: None }
    }

    /// Add details to the score
    pub fn with_details(mut self, details: impl Into<String>) -> Self {
        self.details = Some(details.into());
        self
    }
}

/// Primary reason for strategy selection
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)] // Some variants are for future use (query hints, etc.)
pub enum StrategyReason {
    /// User override via query hint (/* COLUMNAR */, /* ROW_ORIENTED */)
    QueryHint(String),
    /// Table has columnar storage available
    ColumnarStorageAvailable,
    /// Query matches analytical pattern (aggregates, arithmetic, selective projection)
    AnalyticalPattern,
    /// Simple single-table scan detected
    SingleTableScan,
    /// No FROM clause - expression-only query
    NoFromClause,
    /// Feature not supported by columnar paths
    UnsupportedFeature(String),
    /// Default fallback when no specific reason applies
    Fallback,
}

impl std::fmt::Display for StrategyReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StrategyReason::QueryHint(hint) => write!(f, "Query hint: {}", hint),
            StrategyReason::ColumnarStorageAvailable => write!(f, "Columnar storage available"),
            StrategyReason::AnalyticalPattern => write!(f, "Analytical pattern detected"),
            StrategyReason::SingleTableScan => write!(f, "Single table scan"),
            StrategyReason::NoFromClause => write!(f, "No FROM clause"),
            StrategyReason::UnsupportedFeature(feature) => {
                write!(f, "Unsupported feature: {}", feature)
            }
            StrategyReason::Fallback => write!(f, "Default fallback"),
        }
    }
}

/// Context required for strategy selection
///
/// This bundles all the information needed to make an execution strategy decision.
pub struct StrategyContext<'a> {
    /// The SELECT statement to analyze
    pub stmt: &'a SelectStmt,
    /// CTE results from outer query (if any)
    pub cte_results: &'a HashMap<String, CteResult>,
    /// Whether native-columnar feature is enabled
    pub native_columnar_enabled: bool,
}

impl<'a> StrategyContext<'a> {
    /// Create a new strategy context
    pub fn new(
        stmt: &'a SelectStmt,
        cte_results: &'a HashMap<String, CteResult>,
        native_columnar_enabled: bool,
    ) -> Self {
        Self { stmt, cte_results, native_columnar_enabled }
    }
}

/// Choose the optimal execution strategy for a query
///
/// This is the single decision point for execution path selection. It encapsulates
/// all eligibility checks that were previously scattered across multiple methods.
///
/// # Arguments
/// * `ctx` - The strategy context containing the query and execution environment
///
/// # Returns
/// The recommended execution strategy with its selection rationale
///
/// # Example
///
/// ```text
/// let ctx = StrategyContext::new(&stmt, &cte_results, true);
/// let strategy = choose_execution_strategy(&ctx);
///
/// match strategy {
///     ExecutionStrategy::NativeColumnar { table, .. } => {
///         // Execute using zero-copy columnar path
///     }
///     ExecutionStrategy::StandardColumnar { .. } => {
///         // Execute using row-to-batch columnar path
///     }
///     ExecutionStrategy::RowOriented { has_aggregation, .. } => {
///         // Execute using traditional row-by-row path
///     }
///     ExecutionStrategy::ExpressionOnly { .. } => {
///         // Evaluate expressions without FROM
///     }
/// }
/// ```
pub fn choose_execution_strategy(ctx: &StrategyContext<'_>) -> ExecutionStrategy {
    let stmt = ctx.stmt;

    // Check 1: No FROM clause → ExpressionOnly
    if stmt.from.is_none() {
        return ExecutionStrategy::ExpressionOnly {
            score: StrategyScore::new(StrategyReason::NoFromClause),
        };
    }

    // Check 2: Has CTEs or set operations → Row-oriented (columnar paths don't support these yet)
    if !ctx.cte_results.is_empty() || stmt.set_operation.is_some() {
        let reason = if !ctx.cte_results.is_empty() { "CTEs" } else { "set operations" };
        return make_row_oriented(stmt, StrategyReason::UnsupportedFeature(reason.to_string()));
    }

    // Check 3: Try NativeColumnar (highest priority columnar path)
    if let Some(strategy) = try_native_columnar_strategy(ctx) {
        return strategy;
    }

    // Check 4: Try StandardColumnar (lower priority, has more restrictions)
    if let Some(strategy) = try_standard_columnar_strategy(ctx) {
        return strategy;
    }

    // Check 5: Default to row-oriented
    make_row_oriented(stmt, StrategyReason::Fallback)
}

/// Try to select NativeColumnar strategy
///
/// Requirements:
/// - native-columnar feature enabled
/// - Single table (no JOINs)
/// - Analytical pattern detected
fn try_native_columnar_strategy(ctx: &StrategyContext<'_>) -> Option<ExecutionStrategy> {
    // Native columnar must be enabled
    if !ctx.native_columnar_enabled {
        return None;
    }

    // Must be a single table scan
    if !is_single_table(ctx.stmt) {
        return None;
    }

    // Must have analytical pattern
    if !has_analytical_pattern(ctx.stmt) {
        return None;
    }

    // Extract table name
    let table_name = match &ctx.stmt.from {
        Some(FromClause::Table { name, .. }) => name.clone(),
        _ => return None,
    };

    Some(ExecutionStrategy::NativeColumnar {
        table: table_name,
        score: StrategyScore::new(StrategyReason::ColumnarStorageAvailable)
            .with_details("Zero-copy SIMD execution from columnar storage"),
    })
}

/// Try to select StandardColumnar strategy
///
/// Requirements:
/// - No GROUP BY (GROUP BY goes through NativeColumnar or RowOriented)
/// - Single table (no JOINs)
/// - Analytical pattern detected
fn try_standard_columnar_strategy(ctx: &StrategyContext<'_>) -> Option<ExecutionStrategy> {
    // GROUP BY is NOT supported in StandardColumnar path
    // It should use NativeColumnar or fall back to RowOriented
    if ctx.stmt.group_by.is_some() {
        return None;
    }

    // Must be a single table scan
    if !is_single_table(ctx.stmt) {
        return None;
    }

    // Must have analytical pattern
    if !has_analytical_pattern(ctx.stmt) {
        return None;
    }

    Some(ExecutionStrategy::StandardColumnar {
        score: StrategyScore::new(StrategyReason::AnalyticalPattern)
            .with_details("SIMD execution with row-to-batch conversion"),
    })
}

/// Create a RowOriented strategy with the given reason
fn make_row_oriented(stmt: &SelectStmt, reason: StrategyReason) -> ExecutionStrategy {
    let has_agg = has_aggregate_functions(stmt) || stmt.having.is_some();
    let has_gb = has_group_by(stmt);

    ExecutionStrategy::RowOriented {
        has_aggregation: has_agg,
        has_group_by: has_gb,
        score: StrategyScore::new(reason),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_ast::{BinaryOperator, Expression, GroupByClause, SelectItem};
    use vibesql_types::SqlValue;

    fn make_simple_table_query(table: &str) -> SelectStmt {
        SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: vec![SelectItem::Wildcard { alias: None }],
            into_table: None,
            into_variables: None,
            from: Some(FromClause::Table {
                name: table.to_string(),
                alias: None,
                column_aliases: None,
            }),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
        }
    }

    fn make_aggregate_query(table: &str) -> SelectStmt {
        SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: vec![SelectItem::Expression {
                expr: Expression::AggregateFunction {
                    name: "SUM".to_string(),
                    distinct: false,
                    args: vec![Expression::BinaryOp {
                        left: Box::new(Expression::ColumnRef {
                            table: None,
                            column: "price".to_string(),
                        }),
                        op: BinaryOperator::Multiply,
                        right: Box::new(Expression::ColumnRef {
                            table: None,
                            column: "quantity".to_string(),
                        }),
                    }],
                },
                alias: None,
            }],
            into_table: None,
            into_variables: None,
            from: Some(FromClause::Table {
                name: table.to_string(),
                alias: None,
                column_aliases: None,
            }),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
        }
    }

    #[test]
    fn test_expression_only_for_no_from() {
        let stmt = SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: vec![SelectItem::Expression {
                expr: Expression::Literal(SqlValue::Integer(42)),
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

        let cte_results = HashMap::new();
        let ctx = StrategyContext::new(&stmt, &cte_results, true);
        let strategy = choose_execution_strategy(&ctx);

        assert!(matches!(strategy, ExecutionStrategy::ExpressionOnly { .. }));
    }

    #[test]
    fn test_row_oriented_for_simple_select() {
        let stmt = make_simple_table_query("users");
        let cte_results = HashMap::new();
        let ctx = StrategyContext::new(&stmt, &cte_results, true);
        let strategy = choose_execution_strategy(&ctx);

        assert!(matches!(strategy, ExecutionStrategy::RowOriented { .. }));
    }

    #[test]
    fn test_native_columnar_for_aggregate_when_enabled() {
        let stmt = make_aggregate_query("orders");
        let cte_results = HashMap::new();
        let ctx = StrategyContext::new(&stmt, &cte_results, true);
        let strategy = choose_execution_strategy(&ctx);

        // Should choose NativeColumnar when feature is enabled and query has analytical pattern
        assert!(matches!(strategy, ExecutionStrategy::NativeColumnar { .. }));
    }

    #[test]
    fn test_standard_columnar_for_aggregate_when_native_disabled() {
        let stmt = make_aggregate_query("orders");
        let cte_results = HashMap::new();
        let ctx = StrategyContext::new(&stmt, &cte_results, false);
        let strategy = choose_execution_strategy(&ctx);

        // Should choose StandardColumnar when native is disabled but query has analytical pattern
        assert!(matches!(strategy, ExecutionStrategy::StandardColumnar { .. }));
    }

    #[test]
    fn test_row_oriented_for_group_by_when_native_disabled() {
        let mut stmt = make_aggregate_query("orders");
        stmt.group_by = Some(GroupByClause::Simple(vec![Expression::ColumnRef {
            table: None,
            column: "region".to_string(),
        }]));

        // With native disabled, GROUP BY queries fall back to row-oriented
        // (StandardColumnar doesn't support GROUP BY)
        let cte_results = HashMap::new();
        let ctx = StrategyContext::new(&stmt, &cte_results, false);
        let strategy = choose_execution_strategy(&ctx);

        assert!(matches!(strategy, ExecutionStrategy::RowOriented { .. }));
    }

    #[test]
    fn test_native_columnar_for_group_by_when_enabled() {
        let mut stmt = make_aggregate_query("orders");
        stmt.group_by = Some(GroupByClause::Simple(vec![Expression::ColumnRef {
            table: None,
            column: "region".to_string(),
        }]));

        // With native enabled, GROUP BY queries can use NativeColumnar
        let cte_results = HashMap::new();
        let ctx = StrategyContext::new(&stmt, &cte_results, true);
        let strategy = choose_execution_strategy(&ctx);

        assert!(matches!(strategy, ExecutionStrategy::NativeColumnar { .. }));
    }

    #[test]
    fn test_strategy_name() {
        let native = ExecutionStrategy::NativeColumnar {
            table: "test".to_string(),
            score: StrategyScore::new(StrategyReason::ColumnarStorageAvailable),
        };
        assert_eq!(native.name(), "NativeColumnar");

        let standard = ExecutionStrategy::StandardColumnar {
            score: StrategyScore::new(StrategyReason::AnalyticalPattern),
        };
        assert_eq!(standard.name(), "StandardColumnar");

        let row = ExecutionStrategy::RowOriented {
            has_aggregation: true,
            has_group_by: false,
            score: StrategyScore::new(StrategyReason::Fallback),
        };
        assert_eq!(row.name(), "RowOriented");

        let expr = ExecutionStrategy::ExpressionOnly {
            score: StrategyScore::new(StrategyReason::NoFromClause),
        };
        assert_eq!(expr.name(), "ExpressionOnly");
    }
}
