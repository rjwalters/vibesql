//! Join order optimization using selectivity-based heuristics
//!
//! This module analyzes predicates to determine optimal join orderings.
//! The goal is to maximize row reduction early in the join chain to minimize
//! intermediate result size during cascade joins.

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
#[derive(Debug, Clone, PartialEq)]
pub struct JoinEdge {
    /// Table name on left side of equijoin
    pub left_table: String,
    /// Column from left table
    pub left_column: String,
    /// Table name on right side of equijoin
    pub right_table: String,
    /// Column from right table
    pub right_column: String,
    /// Join type (INNER, SEMI, ANTI, etc.)
    pub join_type: vibesql_ast::JoinType,
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
    #[allow(dead_code)]
    selectivity: HashMap<String, Selectivity>,
    /// Schema-based column-to-table mapping for resolving unqualified columns
    column_to_table: HashMap<String, String>,
}

impl Default for JoinOrderAnalyzer {
    fn default() -> Self {
        Self::new()
    }
}

impl JoinOrderAnalyzer {
    /// Create a new join order analyzer
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
            edges: Vec::new(),
            selectivity: HashMap::new(),
            column_to_table: HashMap::new(),
        }
    }

    /// Create a new join order analyzer with schema-based column resolution
    pub fn with_column_map(column_to_table: HashMap<String, String>) -> Self {
        Self {
            tables: HashMap::new(),
            edges: Vec::new(),
            selectivity: HashMap::new(),
            column_to_table,
        }
    }

    /// Set the column-to-table mapping for schema-based resolution
    pub fn set_column_map(&mut self, column_to_table: HashMap<String, String>) {
        self.column_to_table = column_to_table;
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
        self.analyze_predicate_with_type(expr, tables, vibesql_ast::JoinType::Inner);
    }

    /// Analyze a predicate with an explicit join type
    pub fn analyze_predicate_with_type(
        &mut self,
        expr: &Expression,
        tables: &HashSet<String>,
        join_type: vibesql_ast::JoinType,
    ) {
        match expr {
            // Recursively handle AND expressions
            Expression::BinaryOp { op: BinaryOperator::And, left, right } => {
                if std::env::var("JOIN_REORDER_VERBOSE").is_ok() {
                    eprintln!("[ANALYZER] Decomposing AND expression");
                }
                self.analyze_predicate_with_type(left, tables, join_type.clone());
                self.analyze_predicate_with_type(right, tables, join_type);
            }
            // Handle OR expressions by extracting common join conditions
            // that appear in ALL branches
            Expression::BinaryOp { op: BinaryOperator::Or, .. } => {
                if std::env::var("JOIN_REORDER_VERBOSE").is_ok() {
                    eprintln!("[ANALYZER] Analyzing OR expression for common join conditions");
                }

                // Collect all OR branches into a list
                let mut branches = Vec::new();
                self.collect_or_branches(expr, &mut branches);

                if std::env::var("JOIN_REORDER_VERBOSE").is_ok() {
                    eprintln!("[ANALYZER] Found {} OR branches", branches.len());
                }

                // Extract edges from each branch
                let mut branch_edges: Vec<Vec<JoinEdge>> = Vec::new();
                for branch in &branches {
                    let mut branch_analyzer = JoinOrderAnalyzer::new();
                    let table_vec: Vec<String> = tables.iter().cloned().collect();
                    branch_analyzer.register_tables(table_vec);
                    branch_analyzer.analyze_predicate(branch, tables);
                    branch_edges.push(branch_analyzer.edges().to_vec());
                }

                // Find edges common to ALL branches
                if !branch_edges.is_empty() {
                    let common_edges = self.find_common_edges(&branch_edges);

                    if std::env::var("JOIN_REORDER_VERBOSE").is_ok() {
                        eprintln!(
                            "[ANALYZER] Found {} common join edges across all OR branches",
                            common_edges.len()
                        );
                    }

                    // Add common edges to our join graph
                    for edge in common_edges {
                        if std::env::var("JOIN_REORDER_VERBOSE").is_ok() {
                            eprintln!(
                                "[ANALYZER] Added common edge from OR: {}.{} = {}.{}",
                                edge.left_table,
                                edge.left_column,
                                edge.right_table,
                                edge.right_column
                            );
                        }
                        self.edges.push(edge);
                    }
                }
            }
            // Handle simple binary equality operations
            Expression::BinaryOp { op: BinaryOperator::Equal, left, right } => {
                let (left_table, left_col) = self.extract_column_ref(left, tables);
                let (right_table, right_col) = self.extract_column_ref(right, tables);

                if std::env::var("JOIN_REORDER_VERBOSE").is_ok() {
                    eprintln!("[ANALYZER] analyze_predicate: left_table={:?}, right_table={:?}, left_col={:?}, right_col={:?}",
                        left_table, right_table, left_col, right_col);
                }

                match (left_table, right_table, left_col, right_col) {
                    // Equijoin: column from one table = column from another
                    (Some(lt), Some(rt), Some(lc), Some(rc)) if lt != rt => {
                        let edge = JoinEdge {
                            left_table: lt.to_lowercase(),
                            left_column: lc.clone(),
                            right_table: rt.to_lowercase(),
                            right_column: rc.clone(),
                            join_type: join_type.clone(),
                        };
                        if std::env::var("JOIN_REORDER_VERBOSE").is_ok() {
                            eprintln!(
                                "[ANALYZER] Added edge: {}.{} = {}.{} (join_type: {:?})",
                                lt, lc, rt, rc, join_type
                            );
                        }
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
                    _ => {
                        if std::env::var("JOIN_REORDER_VERBOSE").is_ok() {
                            eprintln!("[ANALYZER] Skipped predicate (no match)");
                        }
                    }
                }
            }
            // For other operators, analyze for local vs cross-table
            _ => {
                // Conservative: mark as complex, don't try to optimize
            }
        }
    }

    /// Collect all branches of an OR expression into a flat list
    /// Handles nested ORs by flattening them: (A OR B) OR C => [A, B, C]
    #[allow(clippy::only_used_in_recursion)]
    fn collect_or_branches(&self, expr: &Expression, branches: &mut Vec<Expression>) {
        match expr {
            Expression::BinaryOp { op: BinaryOperator::Or, left, right } => {
                // Recursively collect from both sides
                self.collect_or_branches(left, branches);
                self.collect_or_branches(right, branches);
            }
            _ => {
                // Leaf node - add to branches
                branches.push(expr.clone());
            }
        }
    }

    /// Find join edges that appear in ALL branches
    /// An edge is common if it has the same tables and columns in every branch
    fn find_common_edges(&self, branch_edges: &[Vec<JoinEdge>]) -> Vec<JoinEdge> {
        if branch_edges.is_empty() {
            return Vec::new();
        }

        // Start with edges from first branch
        let mut common_edges = Vec::new();
        let first_branch = &branch_edges[0];

        for edge in first_branch {
            // Check if this edge appears in all other branches
            let appears_in_all = branch_edges[1..]
                .iter()
                .all(|branch| branch.iter().any(|e| self.edges_match(e, edge)));

            if appears_in_all {
                common_edges.push(edge.clone());
            }
        }

        common_edges
    }

    /// Check if two edges represent the same join condition
    /// Handles both (A=B) and (B=A) as equivalent
    fn edges_match(&self, e1: &JoinEdge, e2: &JoinEdge) -> bool {
        // Direct match: left-to-left, right-to-right
        let direct = e1.left_table.eq_ignore_ascii_case(&e2.left_table)
            && e1.left_column.eq_ignore_ascii_case(&e2.left_column)
            && e1.right_table.eq_ignore_ascii_case(&e2.right_table)
            && e1.right_column.eq_ignore_ascii_case(&e2.right_column);

        // Reverse match: left-to-right, right-to-left (handles A=B vs B=A)
        let reverse = e1.left_table.eq_ignore_ascii_case(&e2.right_table)
            && e1.left_column.eq_ignore_ascii_case(&e2.right_column)
            && e1.right_table.eq_ignore_ascii_case(&e2.left_table)
            && e1.right_column.eq_ignore_ascii_case(&e2.left_column);

        direct || reverse
    }

    /// Extract table and column info from an expression
    /// Returns (table_name, column_name)
    /// Uses table inference if explicit table prefix is not present
    fn extract_column_ref(
        &self,
        expr: &Expression,
        tables: &HashSet<String>,
    ) -> (Option<String>, Option<String>) {
        match expr {
            Expression::ColumnRef { table, column } => {
                // If explicit table prefix exists, use it
                if let Some(t) = table {
                    return (Some(t.clone()), Some(column.clone()));
                }

                // Otherwise, infer table from column prefix
                let inferred_table = self.infer_table_from_column(column, tables);
                (inferred_table, Some(column.clone()))
            }
            Expression::Literal(_) => (None, None),
            _ => (None, None),
        }
    }

    /// Infer table name from column name using schema-based lookup
    ///
    /// Uses the schema-based column-to-table map to resolve column references.
    /// All column resolution relies solely on actual database schema metadata.
    fn infer_table_from_column(&self, column: &str, tables: &HashSet<String>) -> Option<String> {
        // Schema-based lookup only - no heuristic fallbacks
        if self.column_to_table.is_empty() {
            if std::env::var("JOIN_REORDER_VERBOSE").is_ok() {
                eprintln!(
                    "[ANALYZER] Warning: No column-to-table map available for column {}",
                    column
                );
            }
            return None;
        }

        let col_lower = column.to_lowercase();
        if let Some(table) = self.column_to_table.get(&col_lower) {
            if std::env::var("JOIN_REORDER_VERBOSE").is_ok() {
                eprintln!("[ANALYZER] Schema lookup: {} -> {}", column, table);
            }
            // Verify the table is in our set (could be aliased)
            if tables.contains(table) {
                return Some(table.clone());
            }
            // Try case-insensitive match
            for t in tables {
                if t.eq_ignore_ascii_case(table) {
                    return Some(t.clone());
                }
            }
            if std::env::var("JOIN_REORDER_VERBOSE").is_ok() {
                eprintln!("[ANALYZER] Warning: Table {} not in tables set {:?}", table, tables);
            }
        } else if std::env::var("JOIN_REORDER_VERBOSE").is_ok() {
            eprintln!(
                "[ANALYZER] Warning: Column {} not found in schema map (available: {:?})",
                col_lower,
                self.column_to_table.keys().take(10).collect::<Vec<_>>()
            );
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
            join_type: vibesql_ast::JoinType::Inner,
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
            join_type: vibesql_ast::JoinType::Inner,
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
            join_type: vibesql_ast::JoinType::Inner,
        });
        analyzer.edges.push(JoinEdge {
            left_table: "t2".to_string(),
            left_column: "id".to_string(),
            right_table: "t3".to_string(),
            right_column: "id".to_string(),
            join_type: vibesql_ast::JoinType::Inner,
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
            join_type: vibesql_ast::JoinType::Inner,
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
            join_type: vibesql_ast::JoinType::Inner,
        });

        // Should find condition even with case differences
        let condition = analyzer.get_join_condition("T1", "T2");
        assert!(condition.is_some());
    }
}
