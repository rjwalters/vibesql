//! WHERE clause predicate pushdown optimizer
//!
//! This module decomposes WHERE clauses into three categories of predicates:
//! 1. **Table-local predicates**: Reference only one table (can be applied at scan time)
//! 2. **Equijoin conditions**: Simple equality between two tables (apply during join)

//! 3. **Complex predicates**: Multiple tables or non-equality (apply after all joins)
//!
//! The goal is to filter rows as early as possible before intermediate Cartesian products
//! can explode in memory.
//!
//! ## Example
//!
//! Given query:
//! ```sql
//! SELECT * FROM t1, t2, t3
//! WHERE t1.a = 5 AND t1.a = t2.b AND t2.c > 10 AND t3.d IS NOT NULL
//! ```
//!
//! Decomposition:
//! - Table-local: `t1.a = 5`, `t2.c > 10`, `t3.d IS NOT NULL`
//! - Equijoin: `t1.a = t2.b`
//! - Complex: (none)
//!
//! Execution order:
//! 1. Scan t1, apply `t1.a = 5` → 2 rows
//! 2. Scan t2, apply `t2.c > 10` → 3 rows
//! 3. Join t1 with t2 on `t1.a = t2.b` → 5 rows (not 6)
//! 4. Scan t3, apply `t3.d IS NOT NULL` → 8 rows
//! 5. Join result with t3 → 40 rows (not 48)

mod classification;
mod or_conditions;
mod table_refs;

#[cfg(test)]
mod tests;

use std::collections::{HashMap, HashSet};

use vibesql_ast::Expression;

use crate::schema::CombinedSchema;

// Re-exports
pub(crate) use classification::classify_predicate_branch;
pub(crate) use or_conditions::{
    combine_predicates_with_and, extract_implied_filters_from_or_predicates,
};
pub(crate) use table_refs::{extract_referenced_tables_branch, flatten_conjuncts};

// ============================================================================
// Unified WHERE Clause Decomposition API
// ============================================================================

/// Classification of a WHERE clause predicate (branch version)
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq)]
pub enum PredicateCategory {
    /// Predicate references only a single table - can be pushed to table scan
    /// Example: `t1.a = 5` or `t2.name LIKE 'foo%'`
    TableLocal {
        /// Name of the referenced table
        table_name: String,
        /// The predicate expression
        predicate: vibesql_ast::Expression,
    },

    /// Simple equality between columns of two tables - can be used as join condition
    /// Example: `t1.a = t2.b`
    EquiJoin {
        /// Left table and column
        left_table: String,
        left_column: String,
        /// Right table and column
        right_table: String,
        right_column: String,
        /// The full equality expression
        predicate: vibesql_ast::Expression,
    },

    /// Complex predicate - must be applied after joins
    /// Examples: `t1.a + t2.b > 10`, `t1.a = t2.b OR t1.c = t3.d`, etc.
    Complex {
        /// The full predicate expression
        predicate: vibesql_ast::Expression,
        /// Table names referenced in this predicate
        referenced_tables: HashSet<String>,
    },
}

/// Result of decomposing a WHERE clause (branch version)
#[derive(Debug, Clone)]
pub struct PredicateDecomposition {
    /// Table-local predicates grouped by table name
    pub table_local_predicates: HashMap<String, Vec<vibesql_ast::Expression>>,
    /// Equijoin conditions
    pub equijoin_conditions: Vec<(String, String, String, String, vibesql_ast::Expression)>,
    /// Complex predicates that cannot be pushed down
    pub complex_predicates: Vec<vibesql_ast::Expression>,
}

impl PredicateDecomposition {
    /// Create an empty decomposition
    pub fn empty() -> Self {
        Self {
            table_local_predicates: HashMap::new(),
            equijoin_conditions: Vec::new(),
            complex_predicates: Vec::new(),
        }
    }

    /// Check if this decomposition has any predicates
    pub fn is_empty(&self) -> bool {
        self.table_local_predicates.is_empty()
            && self.equijoin_conditions.is_empty()
            && self.complex_predicates.is_empty()
    }

    /// Rebuild a WHERE clause from all categories (for validation/debugging)
    #[allow(dead_code)]
    pub fn rebuild_where_clause(&self) -> Option<vibesql_ast::Expression> {
        let mut all_predicates = Vec::new();

        // Add table-local predicates
        for predicates in self.table_local_predicates.values() {
            all_predicates.extend(predicates.clone());
        }

        // Add equijoin conditions
        for (_, _, _, _, expr) in &self.equijoin_conditions {
            all_predicates.push(expr.clone());
        }

        // Add complex predicates
        all_predicates.extend(self.complex_predicates.clone());

        if all_predicates.is_empty() {
            return None;
        }

        // Combine all predicates with AND
        Some(combine_predicates_with_and(all_predicates))
    }
}

/// Decompose a WHERE clause into pushable and non-pushable predicates (branch version)
pub fn decompose_where_clause(
    where_expr: Option<&vibesql_ast::Expression>,
    from_schema: &CombinedSchema,
) -> Result<PredicateDecomposition, String> {
    let mut decomposition = PredicateDecomposition::empty();

    let Some(expr) = where_expr else {
        return Ok(decomposition);
    };

    // Flatten the WHERE clause into CNF (Conjunctive Normal Form)
    // i.e., split on top-level AND operators
    let conjuncts = flatten_conjuncts(expr);

    for conjunct in conjuncts {
        classify_predicate_branch(&conjunct, from_schema, &mut decomposition)?;
    }

    // Extract implied single-table filters from OR predicates
    // This is critical for queries like TPC-H Q7 where:
    //   (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY') OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
    // implies: n1.n_name IN ('FRANCE', 'GERMANY') AND n2.n_name IN ('FRANCE', 'GERMANY')
    extract_implied_filters_from_or_predicates(&mut decomposition, from_schema);

    Ok(decomposition)
}

// ============================================================================
// Compatibility Functions (for backward compatibility)
// ============================================================================

/// Combine multiple expressions with AND operator
///
/// Public wrapper around combine_predicates_with_and for external use.
pub fn combine_with_and(expressions: Vec<Expression>) -> Option<Expression> {
    match expressions.len() {
        0 => None,
        _ => Some(combine_predicates_with_and(expressions)),
    }
}
