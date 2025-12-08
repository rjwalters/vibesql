//! EXPLAIN statement execution
//!
//! This module provides the ExplainExecutor for analyzing query execution plans.
//! It shows information about:
//! - Table scans vs index scans
//! - Skip-scan optimization (non-prefix index usage)
//! - Join types and order
//! - Filter pushdown information
//! - Estimated row counts (when statistics are available)

use std::fmt::Write;
use vibesql_ast::{ExplainFormat, ExplainStmt, SelectStmt, Statement};
use vibesql_storage::Database;

use crate::errors::ExecutorError;
use crate::optimizer::index_planner::IndexPlanner;
use crate::select::scan::index_scan::cost_based_index_selection;

/// Represents a single node in the query execution plan
#[derive(Debug, Clone)]
pub struct PlanNode {
    /// Type of operation (e.g., "Seq Scan", "Index Scan", "Hash Join")
    pub operation: String,
    /// Target object (e.g., table name, index name)
    pub object: Option<String>,
    /// Additional details about this operation
    pub details: Vec<String>,
    /// Estimated rows (if statistics available)
    pub estimated_rows: Option<f64>,
    /// Child nodes in the plan tree
    pub children: Vec<PlanNode>,
}

impl PlanNode {
    fn new(operation: &str) -> Self {
        PlanNode {
            operation: operation.to_string(),
            object: None,
            details: Vec::new(),
            estimated_rows: None,
            children: Vec::new(),
        }
    }

    fn with_object(mut self, object: &str) -> Self {
        self.object = Some(object.to_string());
        self
    }

    fn with_detail(mut self, detail: String) -> Self {
        self.details.push(detail);
        self
    }

    fn with_estimated_rows(mut self, rows: f64) -> Self {
        self.estimated_rows = Some(rows);
        self
    }

    fn add_child(&mut self, child: PlanNode) {
        self.children.push(child);
    }
}

/// Result of EXPLAIN operation
#[derive(Debug)]
pub struct ExplainResult {
    /// Root node of the execution plan
    pub plan: PlanNode,
    /// Output format
    pub format: ExplainFormat,
}

impl ExplainResult {
    /// Format the plan as text output
    pub fn to_text(&self) -> String {
        let mut output = String::new();
        format_node_text(&self.plan, 0, &mut output);
        output
    }

    /// Format the plan as JSON output
    pub fn to_json(&self) -> String {
        format_node_json(&self.plan)
    }
}

fn format_node_text(node: &PlanNode, depth: usize, output: &mut String) {
    let indent = "  ".repeat(depth);
    let arrow = if depth > 0 { "-> " } else { "" };

    // Format the main operation line
    let mut line = format!("{}{}{}", indent, arrow, node.operation);

    if let Some(ref obj) = node.object {
        write!(line, " on {}", obj).unwrap();
    }

    if let Some(rows) = node.estimated_rows {
        write!(line, "  (rows={:.0})", rows).unwrap();
    }

    writeln!(output, "{}", line).unwrap();

    // Format details
    for detail in &node.details {
        writeln!(output, "{}      {}", indent, detail).unwrap();
    }

    // Format children
    for child in &node.children {
        format_node_text(child, depth + 1, output);
    }
}

fn format_node_json(node: &PlanNode) -> String {
    let mut parts = vec![format!("\"operation\": \"{}\"", node.operation)];

    if let Some(ref obj) = node.object {
        parts.push(format!("\"object\": \"{}\"", obj));
    }

    if !node.details.is_empty() {
        let details: Vec<String> = node.details.iter().map(|d| format!("\"{}\"", d)).collect();
        parts.push(format!("\"details\": [{}]", details.join(", ")));
    }

    if let Some(rows) = node.estimated_rows {
        parts.push(format!("\"estimated_rows\": {:.0}", rows));
    }

    if !node.children.is_empty() {
        let children: Vec<String> = node.children.iter().map(format_node_json).collect();
        parts.push(format!("\"children\": [{}]", children.join(", ")));
    }

    format!("{{{}}}", parts.join(", "))
}

/// Executor for EXPLAIN statements
pub struct ExplainExecutor;

impl ExplainExecutor {
    /// Execute an EXPLAIN statement
    pub fn execute(
        stmt: &ExplainStmt,
        database: &Database,
    ) -> Result<ExplainResult, ExecutorError> {
        let plan = match stmt.statement.as_ref() {
            Statement::Select(select_stmt) => Self::explain_select(select_stmt, database)?,
            Statement::Insert(_) => {
                PlanNode::new("Insert").with_detail("Inserts rows into target table".to_string())
            }
            Statement::Update(_) => PlanNode::new("Update")
                .with_detail("Updates rows matching WHERE clause".to_string()),
            Statement::Delete(_) => PlanNode::new("Delete")
                .with_detail("Deletes rows matching WHERE clause".to_string()),
            _ => {
                return Err(ExecutorError::Other(
                    "EXPLAIN only supports SELECT, INSERT, UPDATE, DELETE statements".to_string(),
                ));
            }
        };

        Ok(ExplainResult { plan, format: stmt.format.clone() })
    }

    /// Generate execution plan for a SELECT statement
    fn explain_select(stmt: &SelectStmt, database: &Database) -> Result<PlanNode, ExecutorError> {
        let mut root = PlanNode::new("Select");

        // Analyze FROM clause
        if let Some(ref from_clause) = stmt.from {
            let scan_node = Self::explain_from_clause(
                from_clause,
                &stmt.where_clause,
                &stmt.order_by,
                database,
            )?;
            root.add_child(scan_node);
        }

        // Add WHERE clause info
        if stmt.where_clause.is_some() {
            root.details.push("Filter: <where clause>".to_string());
        }

        // Add GROUP BY info
        if stmt.group_by.is_some() {
            root.details.push("Group: <group by clause>".to_string());
        }

        // Add ORDER BY info
        if stmt.order_by.is_some() {
            root.details.push("Sort: <order by clause>".to_string());
        }

        // Add LIMIT info
        if let Some(limit) = stmt.limit {
            root.details.push(format!("Limit: {}", limit));
        }

        Ok(root)
    }

    /// Generate plan node for FROM clause
    fn explain_from_clause(
        from: &vibesql_ast::FromClause,
        where_clause: &Option<vibesql_ast::Expression>,
        order_by: &Option<Vec<vibesql_ast::OrderByItem>>,
        database: &Database,
    ) -> Result<PlanNode, ExecutorError> {
        match from {
            vibesql_ast::FromClause::Table { name, alias, .. } => {
                Self::explain_table_scan(name, alias.as_deref(), where_clause, order_by, database)
            }
            vibesql_ast::FromClause::Join { left, right, join_type, condition, natural } => {
                let join_name = match join_type {
                    vibesql_ast::JoinType::Inner => "Inner Join",
                    vibesql_ast::JoinType::LeftOuter => "Left Outer Join",
                    vibesql_ast::JoinType::RightOuter => "Right Outer Join",
                    vibesql_ast::JoinType::FullOuter => "Full Outer Join",
                    vibesql_ast::JoinType::Cross => "Cross Join",
                    vibesql_ast::JoinType::Semi => "Semi Join",
                    vibesql_ast::JoinType::Anti => "Anti Join",
                };

                let mut join_node = PlanNode::new(join_name);

                if *natural {
                    join_node.details.push("NATURAL join".to_string());
                }

                if condition.is_some() {
                    join_node.details.push("Join condition: <on clause>".to_string());
                }

                // Add left child
                let left_child = Self::explain_from_clause(left, where_clause, order_by, database)?;
                join_node.add_child(left_child);

                // Add right child (no WHERE pushdown for right side in simple case)
                let right_child = Self::explain_from_clause(right, &None, &None, database)?;
                join_node.add_child(right_child);

                Ok(join_node)
            }
            vibesql_ast::FromClause::Subquery { query, alias, .. } => {
                let mut subquery_node = PlanNode::new("Subquery");
                subquery_node.object = Some(format!("AS {}", alias));

                let child = Self::explain_select(query, database)?;
                subquery_node.add_child(child);

                Ok(subquery_node)
            }
        }
    }

    /// Generate plan node for table scan (sequential or index)
    fn explain_table_scan(
        table_name: &str,
        alias: Option<&str>,
        where_clause: &Option<vibesql_ast::Expression>,
        order_by: &Option<Vec<vibesql_ast::OrderByItem>>,
        database: &Database,
    ) -> Result<PlanNode, ExecutorError> {
        // First check for regular index scan
        let index_info = cost_based_index_selection(
            table_name,
            where_clause.as_ref(),
            order_by.as_ref().map(|v| v.as_slice()),
            database,
        );

        // If no regular index scan, check for skip-scan optimization
        let skip_scan_plan = if index_info.is_none() {
            if let Some(where_expr) = where_clause {
                let planner = IndexPlanner::new(database);
                planner.plan_skip_scan(table_name, where_expr)
            } else {
                None
            }
        } else {
            None
        };

        let mut node = if let Some(skip_plan) = skip_scan_plan {
            // Skip-scan detected - display skip-scan specific information
            let skip_info = skip_plan.skip_scan_info.as_ref().unwrap();

            let mut skip_node = PlanNode::new("Skip Scan").with_object(table_name);
            skip_node.details.push(format!("Using index: {}", skip_plan.index_name));
            skip_node.details.push(format!(
                "Skip columns: {} (cardinality: {})",
                skip_info.prefix_columns.join(", "),
                skip_info.prefix_cardinality
            ));
            skip_node.details.push(format!("Filter column: {}", skip_info.filter_column));
            skip_node.details.push(format!("Estimated cost: {:.2}", skip_info.estimated_cost));

            skip_node
        } else if let Some((index_name, sorted_cols)) = index_info {
            let mut idx_node = PlanNode::new("Index Scan").with_object(table_name);
            idx_node.details.push(format!("Using index: {}", index_name));

            if let Some(cols) = sorted_cols {
                let col_strs: Vec<String> = cols
                    .iter()
                    .map(|(col, dir)| {
                        format!(
                            "{} {}",
                            col,
                            match dir {
                                vibesql_ast::OrderDirection::Asc => "ASC",
                                vibesql_ast::OrderDirection::Desc => "DESC",
                            }
                        )
                    })
                    .collect();
                idx_node.details.push(format!("Sorted by: {}", col_strs.join(", ")));
            }

            idx_node
        } else {
            PlanNode::new("Seq Scan").with_object(table_name)
        };

        // Add alias if present
        if let Some(a) = alias {
            node.details.push(format!("Alias: {}", a));
        }

        // Add row estimate if table exists
        if let Some(table) = database.get_table(table_name) {
            let row_count = table.row_count();
            node = node.with_estimated_rows(row_count as f64);
        }

        Ok(node)
    }
}
