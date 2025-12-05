//! Predicate plan abstraction for WHERE clause optimization
//!
//! This module wraps `PredicateDecomposition` with a cleaner API to avoid
//! redundant WHERE clause decomposition during query execution.
//!
//! ## Problem
//! Previously, `decompose_where_clause()` was called 7+ times for a 3-table join,
//! creating O(n²) complexity. Each module (table scan, join scan, etc.) called it
//! independently.
//!
//! ## Solution
//! `PredicatePlan` wraps the decomposition result and is computed exactly once at
//! the beginning of query execution, then threaded through the execution context.

use std::collections::HashMap;
use vibesql_ast::Expression;

use super::selectivity::order_predicates_by_selectivity;
use super::where_pushdown::{decompose_where_clause, PredicateDecomposition};
use crate::schema::CombinedSchema;

/// Structured plan for predicate application during query execution
///
/// This wraps `PredicateDecomposition` with convenient accessor methods to eliminate
/// redundant WHERE clause decomposition calls.
#[derive(Debug, Clone)]
pub struct PredicatePlan {
    /// The underlying predicate decomposition
    decomposition: PredicateDecomposition,
}

#[allow(dead_code)]
impl PredicatePlan {
    /// Create an empty predicate plan (for queries without WHERE clause)
    pub fn empty() -> Self {
        Self { decomposition: PredicateDecomposition::empty() }
    }

    /// Create a predicate plan from a WHERE clause expression
    ///
    /// This calls `decompose_where_clause()` exactly once and caches the result.
    pub fn from_where_clause(
        where_expr: Option<&Expression>,
        schema: &CombinedSchema,
    ) -> Result<Self, String> {
        let decomposition = decompose_where_clause(where_expr, schema)?;
        Ok(Self { decomposition })
    }

    /// Get table-local predicates for a specific table
    ///
    /// Returns an empty slice if there are no predicates for this table.
    /// Note: Table names are normalized to lowercase for case-insensitive lookup.
    pub fn get_table_filters(&self, table_name: &str) -> &[Expression] {
        // Normalize to lowercase for case-insensitive SQL table name lookup
        let normalized = table_name.to_lowercase();
        self.decomposition
            .table_local_predicates
            .get(&normalized)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Get table-local predicates ordered by selectivity (most selective first)
    ///
    /// Uses table statistics to estimate predicate selectivity and reorder them
    /// for optimal filter performance. If statistics are unavailable, returns
    /// predicates in parse order.
    ///
    /// # Arguments
    /// * `table_name` - The table to get predicates for
    /// * `stats` - Optional table statistics for selectivity estimation
    ///
    /// # Returns
    /// Vector of predicates ordered by selectivity (owned, not borrowed)
    pub fn get_table_filters_ordered(
        &self,
        table_name: &str,
        stats: Option<&vibesql_storage::statistics::TableStatistics>,
    ) -> Vec<Expression> {
        let predicates = self.get_table_filters(table_name);

        if predicates.is_empty() {
            return Vec::new();
        }

        // If statistics are available, order by selectivity
        if let Some(table_stats) = stats {
            order_predicates_by_selectivity(predicates.to_vec(), table_stats)
        } else {
            // No statistics: return in parse order
            predicates.to_vec()
        }
    }

    /// Check if there are any table-local predicates for a given table
    /// Note: Table names are normalized to lowercase for case-insensitive lookup.
    pub fn has_table_filters(&self, table_name: &str) -> bool {
        // Normalize to lowercase for case-insensitive SQL table name lookup
        let normalized = table_name.to_lowercase();
        self.decomposition
            .table_local_predicates
            .get(&normalized)
            .is_some_and(|preds| !preds.is_empty())
    }

    /// Get all equijoin conditions
    ///
    /// Returns (left_table, left_column, right_table, right_column, expression) tuples.
    pub fn get_equijoin_conditions(&self) -> &[(String, String, String, String, Expression)] {
        &self.decomposition.equijoin_conditions
    }

    /// Get all complex predicates (must be applied after joins)
    pub fn get_complex_predicates(&self) -> &[Expression] {
        &self.decomposition.complex_predicates
    }

    /// Get all table-local predicates (for all tables)
    pub fn get_all_table_predicates(&self) -> &HashMap<String, Vec<Expression>> {
        &self.decomposition.table_local_predicates
    }

    /// Check if this plan has any predicates at all
    pub fn is_empty(&self) -> bool {
        self.decomposition.is_empty()
    }

    /// Get the underlying decomposition (for compatibility)
    pub fn decomposition(&self) -> &PredicateDecomposition {
        &self.decomposition
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_plan() {
        let plan = PredicatePlan::empty();
        assert!(plan.is_empty());
        assert_eq!(plan.get_table_filters("t1").len(), 0);
        assert_eq!(plan.get_equijoin_conditions().len(), 0);
        assert_eq!(plan.get_complex_predicates().len(), 0);
    }

    #[test]
    fn test_has_table_filters() {
        let plan = PredicatePlan::empty();
        assert!(!plan.has_table_filters("t1"));
    }
}
