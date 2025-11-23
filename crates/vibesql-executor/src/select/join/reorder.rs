//! Join order optimization using selectivity-based heuristics
//!
//! This module analyzes predicates to determine optimal join orderings.
//! The goal is to maximize row reduction early in the join chain to minimize
//! intermediate result size during cascade joins.
//!
//! ## Example
//!
//! ```text
//! SELECT * FROM t1, t2, t3, t4, t5, t6, t7, t8, t9, t10
//! WHERE a1 = 5                    -- local predicate for t1
//!   AND a1 = b2                   -- equijoin t1-t2
//!   AND a2 = b3                   -- equijoin (t1 ∪ t2)-t3
//!   AND a3 = b4                   -- equijoin (t1 ∪ t2 ∪ t3)-t4
//!   ...
//!
//! Default order (left-to-right cascade):
//! ((((((((((t1 JOIN t2) JOIN t3) JOIN t4) ... JOIN t10)
//!
//! Result: 90-row intermediates at each step (9 rows × 10 rows before equijoin filter)
//!
//! Optimal order (with selectivity awareness):
//! Start with most selective: t1 (filtered to ~1 row by a1=5)
//! Then: t1 JOIN t2 (1 × 10 = 10 intermediate, filtered to ~1 by a1=b2)
//! Then: result JOIN t3 (1 × 10 = 10 intermediate, filtered to ~1 by a2=b3)
//! ...
//!
//! Result: 10-row intermediates maximum (much better memory usage)
//! ```

#![allow(dead_code)]

use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
};

use vibesql_ast::{BinaryOperator, Expression};

/// Information about a table and its predicates
#[derive(Debug, Clone)]
struct TableInfo {
    name: String,
    local_predicates: Vec<Expression>, // Predicates that only reference this table
    local_selectivity: f64,            // Estimated selectivity of local predicates (0.0-1.0)
}

/// Information about an equijoin between two tables
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinEdge {
    /// Table name on left side of equijoin
    pub left_table: String,
    /// Column from left table
    pub left_column: String,
    /// Table name on right side of equijoin
    pub right_table: String,
    /// Column from right table
    pub right_column: String,
}

impl JoinEdge {
    /// Check if this edge involves a specific table
    pub fn involves_table(&self, table: &str) -> bool {
        self.left_table.eq_ignore_ascii_case(table) || self.right_table.eq_ignore_ascii_case(table)
    }

    /// Get the other table in this edge (if input is one side)
    pub fn other_table(&self, table: &str) -> Option<String> {
        if self.left_table.eq_ignore_ascii_case(table) {
            Some(self.right_table.clone())
        } else if self.right_table.eq_ignore_ascii_case(table) {
            Some(self.left_table.clone())
        } else {
            None
        }
    }
}

/// Selectivity information for a predicate
#[derive(Debug, Clone)]
pub struct Selectivity {
    /// Estimated selectivity (0.0 = filters everything, 1.0 = no filtering)
    pub factor: f64,
    /// Type of selectivity (local vs equijoin)
    pub predicate_type: PredicateType,
}

/// Classification of predicates by type
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PredicateType {
    /// Predicate on single table (e.g., a1 > 5)
    Local,
    /// Equijoin between two tables (e.g., a1 = b2)
    Equijoin,
    /// Complex predicate involving multiple tables
    Complex,
}

/// Analyzes join chains and determines optimal join ordering
#[derive(Debug, Clone)]
pub struct JoinOrderAnalyzer {
    /// Mapping from table name to table info
    tables: HashMap<String, TableInfo>,
    /// List of equijoin edges discovered
    edges: Vec<JoinEdge>,
    /// Selectivity information for each predicate
    selectivity: HashMap<String, Selectivity>,
}

impl Default for JoinOrderAnalyzer {
    fn default() -> Self {
        Self::new()
    }
}

impl JoinOrderAnalyzer {
    /// Create a new join order analyzer
    pub fn new() -> Self {
        Self { tables: HashMap::new(), edges: Vec::new(), selectivity: HashMap::new() }
    }

    /// Register all tables involved in the query
    pub fn register_tables(&mut self, table_names: Vec<String>) {
        for name in table_names {
            self.tables.insert(
                name.to_lowercase(),
                TableInfo {
                    name: name.to_lowercase(),
                    local_predicates: Vec::new(),
                    local_selectivity: 1.0,
                },
            );
        }
    }

    /// Analyze a predicate and extract join edges or local predicates
    pub fn analyze_predicate(&mut self, expr: &Expression, tables: &HashSet<String>) {
        match expr {
            // Handle AND chains recursively (critical for WHERE clause analysis)
            Expression::BinaryOp { op: BinaryOperator::And, left, right } => {
                self.analyze_predicate(left, tables);
                self.analyze_predicate(right, tables);
            }
            // Only handle simple binary equality operations
            Expression::BinaryOp { op: BinaryOperator::Equal, left, right } => {
                let (left_table, left_col) = self.extract_column_ref(left, tables);
                let (right_table, right_col) = self.extract_column_ref(right, tables);

                match (left_table, right_table, left_col, right_col) {
                    // Equijoin: column from one table = column from another
                    (Some(lt), Some(rt), Some(lc), Some(rc)) if lt != rt => {
                        if std::env::var("JOIN_REORDER_VERBOSE").is_ok() {
                            eprintln!("[JOIN_ANALYZER] Adding edge: {}.{} = {}.{}",
                                lt, lc, rt, rc);
                        }
                        let edge = JoinEdge {
                            left_table: lt.to_lowercase(),
                            left_column: lc,
                            right_table: rt.to_lowercase(),
                            right_column: rc,
                        };
                        self.edges.push(edge);
                    }
                    // Local predicate: column = constant
                    (Some(table), None, Some(_col), _) => {
                        if let Some(table_info) = self.tables.get_mut(&table.to_lowercase()) {
                            table_info.local_predicates.push(expr.clone());
                            // Heuristic: equality predicate has ~10% selectivity
                            table_info.local_selectivity *= 0.1;
                        }
                    }
                    _ => {}
                }
            }
            // For other operators, analyze for local vs cross-table
            _ => {
                // Conservative: mark as complex, don't try to optimize
            }
        }
    }

    /// Extract table and column info from an expression
    /// Returns (table_name, column_name)
    fn extract_column_ref(
        &self,
        expr: &Expression,
        tables: &HashSet<String>,
    ) -> (Option<String>, Option<String>) {
        match expr {
            Expression::ColumnRef { table: Some(t), column } => {
                (Some(t.clone()), Some(column.clone()))
            }
            Expression::ColumnRef { table: None, column } => {
                // Infer table from column prefix (e.g., "P_PARTKEY" -> "PART")
                let inferred_table = self.infer_table_from_column(column, tables);
                (inferred_table, Some(column.clone()))
            }
            Expression::Literal(_) => (None, None),
            _ => (None, None),
        }
    }

    /// Infer table name from column prefix (e.g., "P_PARTKEY" -> "PART")
    ///
    /// This mirrors the logic in extract_where_equijoins to ensure consistency.
    fn infer_table_from_column(&self, column: &str, tables: &HashSet<String>) -> Option<String> {
        let prefix = column.split('_').next().unwrap_or("").to_uppercase();
        if prefix.is_empty() {
            return None;
        }

        // Special case: Handle common TPC-H multi-character abbreviations
        if prefix == "PS" {
            for table in tables {
                if table.to_uppercase() == "PARTSUPP" {
                    return Some(table.clone());
                }
            }
        }

        // Collect all matching tables (both exact and prefix matches)
        let mut exact_matches = Vec::new();
        let mut prefix_matches = Vec::new();

        for table in tables {
            let table_upper = table.to_uppercase();
            if table_upper == prefix {
                exact_matches.push(table.clone());
            } else if table_upper.starts_with(&prefix) {
                prefix_matches.push((table.clone(), table.len()));
            }
        }

        // Prefer exact match (e.g., "P" -> "PART" even if "PARTSUPP" exists)
        if !exact_matches.is_empty() {
            return exact_matches.into_iter().next();
        }

        // For prefix matches:
        // - Single-character prefixes: prefer shortest match (e.g., "P" -> "PART" not "PARTSUPP")
        // - Multi-character prefixes: prefer longest match (e.g., "PAR" -> "PARTSUPP" not "PART")
        if prefix_matches.len() == 1 {
            return prefix_matches.into_iter().map(|(t, _)| t).next();
        } else if prefix_matches.len() > 1 {
            if prefix.len() == 1 {
                prefix_matches.sort_by_key(|(_, len)| *len);
            } else {
                prefix_matches.sort_by_key(|(_, len)| std::cmp::Reverse(*len));
            }
            return prefix_matches.into_iter().map(|(t, _)| t).next();
        }

        None
    }

    /// Find all tables that have local predicates (highest selectivity filters)
    pub fn find_most_selective_tables(&self) -> Vec<String> {
        let mut tables: Vec<_> =
            self.tables.values().filter(|t| !t.local_predicates.is_empty()).collect();

        // Sort by selectivity (most selective first)
        tables.sort_by(|a, b| {
            a.local_selectivity.partial_cmp(&b.local_selectivity).unwrap_or(Ordering::Equal)
        });

        tables.iter().map(|t| t.name.clone()).collect()
    }

    /// Build a join chain starting from a seed table
    /// Returns list of tables in optimal join order
    pub fn build_join_chain(&self, seed_table: &str) -> Vec<String> {
        let mut chain = vec![seed_table.to_lowercase()];
        let mut visited = HashSet::new();
        visited.insert(seed_table.to_lowercase());

        // Greedy: follow edges from current table
        while chain.len() < self.tables.len() {
            let current_table = chain[chain.len() - 1].clone();

            // Find an edge from current table
            let mut next_table: Option<String> = None;
            for edge in &self.edges {
                if edge.left_table == current_table && !visited.contains(&edge.right_table) {
                    next_table = Some(edge.right_table.clone());
                    break;
                } else if edge.right_table == current_table && !visited.contains(&edge.left_table) {
                    next_table = Some(edge.left_table.clone());
                    break;
                }
            }

            // If no edge found, pick any unvisited table
            if next_table.is_none() {
                for table in self.tables.keys() {
                    if !visited.contains(table) {
                        next_table = Some(table.clone());
                        break;
                    }
                }
            }

            if let Some(table) = next_table {
                chain.push(table.clone());
                visited.insert(table);
            } else {
                break;
            }
        }

        chain
    }

    /// Find optimal join order given all constraints
    ///
    /// Uses heuristic: start with most selective local filters,
    /// then follow equijoin chains
    pub fn find_optimal_order(&self) -> Vec<String> {
        // Find most selective tables (those with local predicates)
        let selective_tables = self.find_most_selective_tables();

        // Start with most selective, build chain
        if let Some(seed) = selective_tables.first() {
            self.build_join_chain(seed)
        } else {
            // Fallback: just use first table
            if let Some(table) = self.tables.keys().next() {
                self.build_join_chain(table)
            } else {
                Vec::new()
            }
        }
    }

    /// Get the equijoin edges that connect two specific tables
    pub fn get_join_condition(
        &self,
        left_table: &str,
        right_table: &str,
    ) -> Option<(String, String)> {
        let left_lower = left_table.to_lowercase();
        let right_lower = right_table.to_lowercase();

        for edge in &self.edges {
            if (edge.left_table == left_lower && edge.right_table == right_lower)
                || (edge.left_table == right_lower && edge.right_table == left_lower)
            {
                return Some((edge.left_column.clone(), edge.right_column.clone()));
            }
        }
        None
    }

    /// Get all equijoin edges
    pub fn edges(&self) -> &[JoinEdge] {
        &self.edges
    }

    /// Get all tables registered in this analyzer
    pub fn tables(&self) -> std::collections::BTreeSet<String> {
        self.tables.keys().cloned().collect()
    }

    /// Add a join edge (for testing)
    #[cfg(test)]
    pub fn add_edge(&mut self, edge: JoinEdge) {
        self.edges.push(edge);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_join_edge_involvement() {
        let edge = JoinEdge {
            left_table: "t1".to_string(),
            left_column: "a".to_string(),
            right_table: "t2".to_string(),
            right_column: "b".to_string(),
        };

        assert!(edge.involves_table("t1"));
        assert!(edge.involves_table("t2"));
        assert!(!edge.involves_table("t3"));
    }

    #[test]
    fn test_join_edge_other_table() {
        let edge = JoinEdge {
            left_table: "t1".to_string(),
            left_column: "a".to_string(),
            right_table: "t2".to_string(),
            right_column: "b".to_string(),
        };

        assert_eq!(edge.other_table("t1"), Some("t2".to_string()));
        assert_eq!(edge.other_table("t2"), Some("t1".to_string()));
        assert_eq!(edge.other_table("t3"), None);
    }

    #[test]
    fn test_basic_chain_detection() {
        let mut analyzer = JoinOrderAnalyzer::new();
        analyzer.register_tables(vec!["t1".to_string(), "t2".to_string(), "t3".to_string()]);

        // Add edges: t1-t2, t2-t3
        analyzer.edges.push(JoinEdge {
            left_table: "t1".to_string(),
            left_column: "id".to_string(),
            right_table: "t2".to_string(),
            right_column: "id".to_string(),
        });
        analyzer.edges.push(JoinEdge {
            left_table: "t2".to_string(),
            left_column: "id".to_string(),
            right_table: "t3".to_string(),
            right_column: "id".to_string(),
        });

        let chain = analyzer.build_join_chain("t1");
        assert_eq!(chain.len(), 3);
        assert_eq!(chain[0], "t1");
        // Should follow edges: t1 -> t2 -> t3
    }

    #[test]
    fn test_most_selective_tables() {
        let mut analyzer = JoinOrderAnalyzer::new();
        analyzer.register_tables(vec!["t1".to_string(), "t2".to_string(), "t3".to_string()]);

        // Create dummy predicates
        let dummy_pred = Expression::Literal(vibesql_types::SqlValue::Integer(5));

        // Add local predicates to t1 (most selective)
        if let Some(table_info) = analyzer.tables.get_mut("t1") {
            table_info.local_predicates.push(dummy_pred.clone());
            table_info.local_selectivity = 0.1;
        }

        // Add local predicate to t2 (less selective)
        if let Some(table_info) = analyzer.tables.get_mut("t2") {
            table_info.local_predicates.push(dummy_pred.clone());
            table_info.local_selectivity = 0.5;
        }

        let selective = analyzer.find_most_selective_tables();
        assert_eq!(selective[0], "t1"); // Most selective first
    }

    #[test]
    fn test_join_condition_lookup() {
        let mut analyzer = JoinOrderAnalyzer::new();
        analyzer.register_tables(vec!["t1".to_string(), "t2".to_string()]);

        analyzer.edges.push(JoinEdge {
            left_table: "t1".to_string(),
            left_column: "id".to_string(),
            right_table: "t2".to_string(),
            right_column: "id".to_string(),
        });

        let condition = analyzer.get_join_condition("t1", "t2");
        assert!(condition.is_some());
        assert_eq!(condition.unwrap(), ("id".to_string(), "id".to_string()));
    }

    #[test]
    fn test_case_insensitive_tables() {
        let mut analyzer = JoinOrderAnalyzer::new();
        analyzer.register_tables(vec!["T1".to_string(), "T2".to_string()]);

        analyzer.edges.push(JoinEdge {
            left_table: "t1".to_string(),
            left_column: "id".to_string(),
            right_table: "t2".to_string(),
            right_column: "id".to_string(),
        });

        // Should find condition even with case differences
        let condition = analyzer.get_join_condition("T1", "T2");
        assert!(condition.is_some());
    }

    #[test]
    fn test_and_expression_handling() {
        // Test that AND expressions are properly recursed
        let mut analyzer = JoinOrderAnalyzer::new();
        analyzer.register_tables(vec!["t1".to_string(), "t2".to_string(), "t3".to_string()]);

        let tables = std::collections::HashSet::from(["t1".to_string(), "t2".to_string(), "t3".to_string()]);

        // Create an AND expression: t1.id = t2.id AND t2.id = t3.id
        let expr1 = Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef {
                table: Some("t1".to_string()),
                column: "id".to_string(),
            }),
            right: Box::new(Expression::ColumnRef {
                table: Some("t2".to_string()),
                column: "id".to_string(),
            }),
        };

        let expr2 = Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef {
                table: Some("t2".to_string()),
                column: "id".to_string(),
            }),
            right: Box::new(Expression::ColumnRef {
                table: Some("t3".to_string()),
                column: "id".to_string(),
            }),
        };

        let and_expr = Expression::BinaryOp {
            op: BinaryOperator::And,
            left: Box::new(expr1),
            right: Box::new(expr2),
        };

        // Analyze the AND expression
        analyzer.analyze_predicate(&and_expr, &tables);

        // Should have extracted both edges
        assert_eq!(analyzer.edges.len(), 2, "Should extract both edges from AND expression");

        // Verify edges were extracted
        let has_t1_t2 = analyzer.edges.iter().any(|e|
            (e.left_table == "t1" && e.right_table == "t2") ||
            (e.left_table == "t2" && e.right_table == "t1")
        );
        let has_t2_t3 = analyzer.edges.iter().any(|e|
            (e.left_table == "t2" && e.right_table == "t3") ||
            (e.left_table == "t3" && e.right_table == "t2")
        );

        assert!(has_t1_t2, "Should have t1-t2 edge");
        assert!(has_t2_t3, "Should have t2-t3 edge");
    }
}
