//! Expression mapper for tracking schema evolution during join execution
//!
//! As joins are executed, the combined schema grows. This module provides utilities
//! to track column positions and validate that expressions can be evaluated in the
//! current schema state.

#![allow(dead_code)]

//!
//! Example:
//! ```text
//! let mut mapper = ExpressionMapper::new();
//! mapper.add_table("users", &users_schema); // cols 0-1
//! mapper.add_table("orders", &orders_schema); // cols 2-3
//!
//! // Now users.id resolves to index 0, orders.user_id to index 3
//! let idx = mapper.resolve_column(Some("users"), "id"); // Some(0)
//! ```

use std::collections::{HashMap, HashSet};

use vibesql_ast::Expression;
use vibesql_catalog::TableSchema;

/// Tracks column positions as schemas evolve during join execution
///
/// Maintains a mapping from (table_name, column_name) to its index in the
/// combined schema. Updates this mapping after each join operation.
#[derive(Debug, Clone)]
pub struct ExpressionMapper {
    /// Map from (table_name, column_name) to index in combined schema
    column_positions: HashMap<(String, String), usize>,
    /// Offset where each table starts in combined schema
    table_offsets: HashMap<String, usize>,
    /// Current total columns
    total_columns: usize,
}

impl ExpressionMapper {
    /// Create a new empty mapper
    pub fn new() -> Self {
        Self { column_positions: HashMap::new(), table_offsets: HashMap::new(), total_columns: 0 }
    }

    /// Add a table to the schema (happens after each join)
    ///
    /// Registers all columns from the table and updates the total column count.
    pub fn add_table(&mut self, table_name: &str, schema: &TableSchema) {
        let offset = self.total_columns;
        let table_lower = table_name.to_lowercase();
        self.table_offsets.insert(table_lower.clone(), offset);

        for (idx, column) in schema.columns.iter().enumerate() {
            let key = (table_lower.clone(), column.name.to_lowercase());
            self.column_positions.insert(key, offset + idx);
        }

        self.total_columns += schema.columns.len();
    }

    /// Resolve a table-qualified column to its index in current schema
    ///
    /// Returns Some(index) if the column exists, None otherwise.
    /// If table is None, tries to find an unqualified column by name.
    pub fn resolve_column(&self, table: Option<&str>, column: &str) -> Option<usize> {
        let table_lower = table.map(|t| t.to_lowercase());
        let column_lower = column.to_lowercase();

        // Try exact match first (table.column)
        if let Some(tbl) = table_lower {
            if let Some(&idx) = self.column_positions.get(&(tbl, column_lower.clone())) {
                return Some(idx);
            }
        }

        // Try unqualified (just column name) - find LEFTMOST match
        // IMPORTANT: For LEFT JOINs, we must resolve to the LEFTMOST table
        // that has the column. Since HashMap iteration order is non-deterministic,
        // we find ALL matches and pick the one with the lowest index.
        let mut best_match: Option<usize> = None;
        for ((_, col), &idx) in &self.column_positions {
            if col == &column_lower {
                match best_match {
                    None => best_match = Some(idx),
                    Some(current_best) if idx < current_best => {
                        best_match = Some(idx);
                    }
                    _ => {}
                }
            }
        }
        best_match
    }

    /// Get all tables currently in the mapper
    pub fn tables(&self) -> HashSet<String> {
        self.table_offsets.keys().cloned().collect()
    }

    /// Get the offset where a specific table starts in the combined schema
    pub fn table_offset(&self, table: &str) -> Option<usize> {
        self.table_offsets.get(&table.to_lowercase()).copied()
    }

    /// Get the total number of columns in the combined schema
    pub fn total_columns(&self) -> usize {
        self.total_columns
    }

    /// Analyze an expression to understand what it references
    ///
    /// Returns information about which tables the expression references
    /// and whether all column references are resolvable in the current schema.
    pub fn analyze_expression(&self, expr: &Expression) -> ExpressionAnalysis {
        let mut tables_referenced = HashSet::new();
        let mut columns_used = Vec::new();
        let mut resolvable = true;

        self.walk_expression(expr, &mut tables_referenced, &mut columns_used, &mut resolvable);

        ExpressionAnalysis { tables_referenced, columns_used, all_resolvable: resolvable }
    }

    /// Check if an expression only references tables in a specific set
    ///
    /// Useful for determining if an expression can be applied after certain joins.
    pub fn expression_refs_only_tables(
        &self,
        expr: &Expression,
        allowed_tables: &HashSet<String>,
    ) -> bool {
        let analysis = self.analyze_expression(expr);
        analysis.all_resolvable
            && analysis.tables_referenced.iter().all(|t| allowed_tables.contains(t))
    }

    /// Walk an expression tree, collecting table and column references
    fn walk_expression(
        &self,
        expr: &Expression,
        tables: &mut HashSet<String>,
        columns: &mut Vec<(String, String)>,
        resolvable: &mut bool,
    ) {
        match expr {
            Expression::ColumnRef { table, column } => {
                let column_lower = column.to_lowercase();
                if let Some(t) = table {
                    let table_lower = t.to_lowercase();
                    tables.insert(table_lower.clone());
                    columns.push((table_lower.clone(), column_lower.clone()));
                    // Check if this column is in current schema
                    if self.resolve_column(Some(t), column).is_none() {
                        *resolvable = false;
                    }
                } else {
                    // Unqualified column - try to resolve
                    if let Some(idx) = self.resolve_column(None, column) {
                        columns.push(("*".to_string(), column_lower.clone()));
                        // Find which table this column belongs to
                        for ((tbl, _col), &col_idx) in &self.column_positions {
                            if col_idx == idx {
                                tables.insert(tbl.clone());
                                break;
                            }
                        }
                    } else {
                        *resolvable = false;
                    }
                }
            }
            Expression::PseudoVariable { pseudo_table, column } => {
                // Pseudo-variables (OLD.column, NEW.column) reference table columns
                // They cannot be resolved in regular SELECTs, only in trigger context
                let pseudo_name = match pseudo_table {
                    vibesql_ast::PseudoTable::Old => "OLD",
                    vibesql_ast::PseudoTable::New => "NEW",
                };
                tables.insert(pseudo_name.to_string());
                columns.push((pseudo_name.to_string(), column.to_lowercase()));
                // Pseudo-variables are not resolvable in regular context
                *resolvable = false;
            }
            Expression::BinaryOp { left, right, .. } => {
                self.walk_expression(left, tables, columns, resolvable);
                self.walk_expression(right, tables, columns, resolvable);
            }
            Expression::UnaryOp { expr: e, .. } => {
                self.walk_expression(e, tables, columns, resolvable);
            }
            Expression::Function { args, .. } => {
                for arg in args {
                    self.walk_expression(arg, tables, columns, resolvable);
                }
            }
            Expression::AggregateFunction { args, .. } => {
                for arg in args {
                    self.walk_expression(arg, tables, columns, resolvable);
                }
            }
            Expression::IsNull { expr: e, .. } => {
                self.walk_expression(e, tables, columns, resolvable);
            }
            Expression::Case { operand, when_clauses, else_result } => {
                if let Some(op) = operand {
                    self.walk_expression(op, tables, columns, resolvable);
                }
                for when_clause in when_clauses {
                    // Handle multiple conditions in this when clause
                    for condition in &when_clause.conditions {
                        self.walk_expression(condition, tables, columns, resolvable);
                    }
                    self.walk_expression(&when_clause.result, tables, columns, resolvable);
                }
                if let Some(else_expr) = else_result {
                    self.walk_expression(else_expr, tables, columns, resolvable);
                }
            }
            Expression::InList { expr: e, values, .. } => {
                self.walk_expression(e, tables, columns, resolvable);
                for value in values {
                    self.walk_expression(value, tables, columns, resolvable);
                }
            }
            Expression::In { expr: e, .. } => {
                self.walk_expression(e, tables, columns, resolvable);
                // Subquery joins can reference outer columns - mark as not reliably resolvable
                *resolvable = false;
            }
            Expression::Between { expr: e, low, high, .. } => {
                self.walk_expression(e, tables, columns, resolvable);
                self.walk_expression(low, tables, columns, resolvable);
                self.walk_expression(high, tables, columns, resolvable);
            }
            Expression::Cast { expr: e, .. } => {
                self.walk_expression(e, tables, columns, resolvable);
            }
            Expression::Position { substring, string, .. } => {
                self.walk_expression(substring, tables, columns, resolvable);
                self.walk_expression(string, tables, columns, resolvable);
            }
            Expression::Trim { removal_char, string, .. } => {
                if let Some(rc) = removal_char {
                    self.walk_expression(rc, tables, columns, resolvable);
                }
                self.walk_expression(string, tables, columns, resolvable);
            }
            Expression::Extract { expr, .. } => {
                self.walk_expression(expr, tables, columns, resolvable);
            }
            Expression::Like { expr: e, pattern, .. } => {
                self.walk_expression(e, tables, columns, resolvable);
                self.walk_expression(pattern, tables, columns, resolvable);
            }
            Expression::ScalarSubquery(_)
            | Expression::Exists { .. }
            | Expression::QuantifiedComparison { .. } => {
                // Subqueries can reference outer columns - mark as not resolvable
                *resolvable = false;
            }
            Expression::WindowFunction { .. } => {
                // Window functions are complex - conservative approach
                *resolvable = false;
            }
            // Literals and other terminal expressions don't reference columns
            Expression::Literal(_)
            | Expression::Wildcard
            | Expression::CurrentDate
            | Expression::CurrentTime { .. }
            | Expression::CurrentTimestamp { .. }
            | Expression::Interval { .. }
            | Expression::Default
            | Expression::DuplicateKeyValue { .. }
            | Expression::NextValue { .. }
            | Expression::SessionVariable { .. } => {
                // No column references
            }
            Expression::MatchAgainst { columns: match_columns, search_modifier, .. } => {
                // Extract column references from columns list
                for column in match_columns {
                    columns.push(("*".to_string(), column.to_lowercase()));
                    if self.resolve_column(None, column).is_none() {
                        *resolvable = false;
                    }
                }
                // Recursively handle the search term
                self.walk_expression(search_modifier, tables, columns, resolvable);
            }
            Expression::Placeholder(_)
            | Expression::NumberedPlaceholder(_)
            | Expression::NamedPlaceholder(_) => {
                // Placeholders don't reference columns
            }
            Expression::Conjunction(children) | Expression::Disjunction(children) => {
                for child in children {
                    self.walk_expression(child, tables, columns, resolvable);
                }
            }
        }
    }
}

/// Analysis of an expression's column references
#[derive(Debug, Clone)]
pub struct ExpressionAnalysis {
    /// Tables referenced in the expression
    pub tables_referenced: HashSet<String>,
    /// Columns used: (table, column) pairs
    pub columns_used: Vec<(String, String)>,
    /// Whether all column references are resolvable in current schema
    pub all_resolvable: bool,
}

impl ExpressionAnalysis {
    /// Check if expression only references columns from a single table
    pub fn is_single_table(&self) -> bool {
        self.tables_referenced.len() == 1
    }

    /// Get the single table if this expression references only one table
    pub fn single_table(&self) -> Option<String> {
        if self.is_single_table() {
            self.tables_referenced.iter().next().cloned()
        } else {
            None
        }
    }
}
