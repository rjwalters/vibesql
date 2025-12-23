//! EXPLAIN statement execution
//!
//! This module provides the ExplainExecutor for analyzing query execution plans.
//! It shows information about:
//! - Table scans vs index scans
//! - Skip-scan optimization (non-prefix index usage)
//! - Join types and order
//! - Filter pushdown information
//! - Estimated row counts (when statistics are available)
//!
//! Supports two output formats:
//! - PostgreSQL-style text output (default for EXPLAIN)
//! - SQLite-style EXPLAIN QUERY PLAN output (for TCL test compatibility)

use std::fmt::Write;

use std::collections::HashSet;

use vibesql_ast::pretty_print::ToSql;
use vibesql_ast::{ExplainFormat, ExplainStmt, Expression, SelectItem, SelectStmt, Statement};
use vibesql_storage::Database;

use crate::{
    errors::ExecutorError, optimizer::index_planner::IndexPlanner,
    select::scan::index_scan::cost_based_index_selection,
};

/// SQLite-style scan type for EQP output
#[derive(Debug, Clone, PartialEq)]
pub enum ScanType {
    /// Sequential scan (SCAN table)
    Scan,
    /// Index search (SEARCH table USING INDEX ...)
    Search,
    /// Covering index scan (SEARCH table USING COVERING INDEX ...)
    CoveringIndex,
    /// Integer primary key lookup (SEARCH table USING INTEGER PRIMARY KEY ...)
    IntegerPrimaryKey,
}

/// Represents index predicate information for SQLite EQP output
#[derive(Debug, Clone)]
pub struct IndexPredicate {
    /// Column name
    pub column: String,
    /// Predicate type: "=", ">", "<", ">=", "<=", etc.
    pub predicate_type: String,
}

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
    /// SQLite-style scan type (for EQP output)
    pub scan_type: Option<ScanType>,
    /// Index name used (for SEARCH operations)
    pub index_name: Option<String>,
    /// Index predicates for SQLite EQP format (e.g., "w=?", "x>? AND x<?")
    pub index_predicates: Vec<IndexPredicate>,
}

impl PlanNode {
    fn new(operation: &str) -> Self {
        PlanNode {
            operation: operation.to_string(),
            object: None,
            details: Vec::new(),
            estimated_rows: None,
            children: Vec::new(),
            scan_type: None,
            index_name: None,
            index_predicates: Vec::new(),
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

    fn with_scan_type(mut self, scan_type: ScanType) -> Self {
        self.scan_type = Some(scan_type);
        self
    }

    fn with_index_name(mut self, index_name: &str) -> Self {
        self.index_name = Some(index_name.to_string());
        self
    }

    fn with_index_predicate(mut self, column: &str, predicate_type: &str) -> Self {
        self.index_predicates.push(IndexPredicate {
            column: column.to_string(),
            predicate_type: predicate_type.to_string(),
        });
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
    /// Format the plan as text output (PostgreSQL-style)
    pub fn to_text(&self) -> String {
        let mut output = String::new();
        format_node_text(&self.plan, 0, &mut output);
        output
    }

    /// Format the plan as JSON output
    pub fn to_json(&self) -> String {
        format_node_json(&self.plan)
    }

    /// Format the plan as SQLite-compatible EXPLAIN QUERY PLAN output
    ///
    /// Produces output like:
    /// ```text
    /// |--SEARCH t1 USING INDEX idx1 (w=?)
    /// `--SCAN t2
    /// ```
    ///
    /// Note: The "QUERY PLAN" column header is added by the CLI/display layer,
    /// not included here to avoid duplication.
    pub fn to_sqlite_eqp(&self) -> String {
        let mut output = String::new();

        // Collect all leaf scan nodes for SQLite-style flat output
        let scan_nodes = collect_scan_nodes(&self.plan);

        for (i, node) in scan_nodes.iter().enumerate() {
            let is_last = i == scan_nodes.len() - 1;
            let prefix = if is_last { "`--" } else { "|--" };
            let line = format_sqlite_eqp_node(node);
            writeln!(output, "{}{}", prefix, line).unwrap();
        }

        output
    }
}

/// Collect all scan/search nodes from the plan tree for SQLite EQP output
fn collect_scan_nodes(node: &PlanNode) -> Vec<&PlanNode> {
    let mut nodes = Vec::new();

    // Check if this is a scan/search node (has scan_type set)
    if node.scan_type.is_some() {
        nodes.push(node);
    }

    // Recursively collect from children
    for child in &node.children {
        nodes.extend(collect_scan_nodes(child));
    }

    nodes
}

/// Format a single node in SQLite EQP style
fn format_sqlite_eqp_node(node: &PlanNode) -> String {
    let table_name = node.object.as_deref().unwrap_or("?");

    match node.scan_type.as_ref() {
        Some(ScanType::Scan) => {
            format!("SCAN {}", table_name)
        }
        Some(ScanType::Search) | Some(ScanType::CoveringIndex) => {
            let index_name = node.index_name.as_deref().unwrap_or("?");
            let covering = if matches!(node.scan_type, Some(ScanType::CoveringIndex)) {
                "COVERING "
            } else {
                ""
            };

            if node.index_predicates.is_empty() {
                format!("SEARCH {} USING {}INDEX {}", table_name, covering, index_name)
            } else {
                let predicates: Vec<String> = node
                    .index_predicates
                    .iter()
                    .map(|p| format!("{}{}?", p.column, p.predicate_type))
                    .collect();
                format!(
                    "SEARCH {} USING {}INDEX {} ({})",
                    table_name,
                    covering,
                    index_name,
                    predicates.join(" AND ")
                )
            }
        }
        Some(ScanType::IntegerPrimaryKey) => {
            if node.index_predicates.is_empty() {
                format!("SEARCH {} USING INTEGER PRIMARY KEY (rowid=?)", table_name)
            } else {
                let predicates: Vec<String> = node
                    .index_predicates
                    .iter()
                    .map(|p| format!("rowid{}?", p.predicate_type))
                    .collect();
                format!(
                    "SEARCH {} USING INTEGER PRIMARY KEY ({})",
                    table_name,
                    predicates.join(" AND ")
                )
            }
        }
        None => {
            // Fallback for nodes without scan_type (e.g., joins)
            node.operation.clone()
        }
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

        // Extract columns needed by the SELECT list for covering index detection
        let needed_columns = extract_select_columns(&stmt.select_list);

        // Analyze FROM clause
        if let Some(ref from_clause) = stmt.from {
            let scan_node = Self::explain_from_clause(
                from_clause,
                &stmt.where_clause,
                &stmt.order_by,
                &needed_columns,
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
        if let Some(ref limit) = stmt.limit {
            root.details.push(format!("Limit: {}", limit.to_sql()));
        }

        Ok(root)
    }

    /// Generate plan node for FROM clause
    fn explain_from_clause(
        from: &vibesql_ast::FromClause,
        where_clause: &Option<vibesql_ast::Expression>,
        order_by: &Option<Vec<vibesql_ast::OrderByItem>>,
        needed_columns: &HashSet<String>,
        database: &Database,
    ) -> Result<PlanNode, ExecutorError> {
        match from {
            vibesql_ast::FromClause::Table { name, alias, .. } => {
                Self::explain_table_scan(
                    name,
                    alias.as_deref(),
                    where_clause,
                    order_by,
                    needed_columns,
                    database,
                )
            }
            vibesql_ast::FromClause::Join {
                left,
                right,
                join_type,
                condition,
                using_columns,
                natural,
            } => {
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

                if let Some(cols) = using_columns {
                    join_node.details.push(format!("USING ({})", cols.join(", ")));
                }

                // Add left child
                let left_child =
                    Self::explain_from_clause(left, where_clause, order_by, needed_columns, database)?;
                join_node.add_child(left_child);

                // Add right child (no WHERE pushdown for right side in simple case)
                let empty_cols = HashSet::new();
                let right_child =
                    Self::explain_from_clause(right, &None, &None, &empty_cols, database)?;
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
            vibesql_ast::FromClause::Values { rows, alias, column_aliases } => {
                let mut values_node = PlanNode::new("Values");
                values_node.object = Some(format!("AS {}", alias));

                values_node.details.push(format!("{} row(s)", rows.len()));

                if let Some(aliases) = column_aliases {
                    values_node.details.push(format!("Columns: {}", aliases.join(", ")));
                }

                Ok(values_node)
            }
        }
    }

    /// Generate plan node for table scan (sequential or index)
    fn explain_table_scan(
        table_name: &str,
        alias: Option<&str>,
        where_clause: &Option<vibesql_ast::Expression>,
        order_by: &Option<Vec<vibesql_ast::OrderByItem>>,
        needed_columns: &HashSet<String>,
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

        // Check if we're using a primary key lookup
        let is_pk_lookup = Self::is_primary_key_lookup(table_name, where_clause, database);

        let mut node = if let Some(skip_plan) = skip_scan_plan {
            // Skip-scan detected - display skip-scan specific information
            let skip_info = skip_plan.skip_scan_info.as_ref().unwrap();

            let mut skip_node = PlanNode::new("Skip Scan")
                .with_object(table_name)
                .with_scan_type(ScanType::Search)
                .with_index_name(&skip_plan.index_name);
            skip_node.details.push(format!("USING INDEX {} ", skip_plan.index_name));
            skip_node.details.push(format!(
                "Skip columns: {} (cardinality: {})",
                skip_info.prefix_columns.join(", "),
                skip_info.prefix_cardinality
            ));
            skip_node.details.push(format!("Filter column: {}", skip_info.filter_column));
            skip_node.details.push(format!("Estimated cost: {:.2}", skip_info.estimated_cost));

            // Add filter column predicate for SQLite EQP
            skip_node = skip_node.with_index_predicate(&skip_info.filter_column, "=");

            skip_node
        } else if let Some((index_name, sorted_cols)) = index_info {
            // Check if this is a covering index (all needed columns are in the index)
            // A covering index requires:
            // 1. All SELECT columns are in the index
            // 2. All WHERE clause columns are also in the index (or evaluable from index)
            let mut all_needed_columns = needed_columns.clone();
            if let Some(where_expr) = where_clause {
                collect_column_refs(where_expr, &mut all_needed_columns);
            }
            let is_covering = is_covering_index(&index_name, &all_needed_columns, database);

            // Determine scan type: PK lookup > Covering Index > Regular Search
            let scan_type = if is_pk_lookup {
                ScanType::IntegerPrimaryKey
            } else if is_covering {
                ScanType::CoveringIndex
            } else {
                ScanType::Search
            };

            let mut idx_node = PlanNode::new("Index Scan")
                .with_object(table_name)
                .with_scan_type(scan_type.clone())
                .with_index_name(&index_name);
            idx_node.details.push(format!("USING INDEX {} ", index_name));

            // Extract predicates from WHERE clause for SQLite EQP format
            if let Some(where_expr) = where_clause {
                let predicates = extract_index_predicates(where_expr, &index_name, database);
                for (col, op) in predicates {
                    idx_node = idx_node.with_index_predicate(&col, &op);
                }
            }

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
            PlanNode::new("Seq Scan").with_object(table_name).with_scan_type(ScanType::Scan)
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

    /// Check if this is a primary key lookup (INTEGER PRIMARY KEY in SQLite)
    fn is_primary_key_lookup(
        table_name: &str,
        where_clause: &Option<Expression>,
        database: &Database,
    ) -> bool {
        let Some(table) = database.get_table(table_name) else {
            return false;
        };

        let Some(pk_columns) = table.schema.primary_key.as_ref() else {
            return false;
        };

        // Only consider single-column integer primary keys
        if pk_columns.len() != 1 {
            return false;
        }

        let pk_col = &pk_columns[0];
        let pk_col_lower = pk_col.to_lowercase();

        // Check if WHERE clause references the primary key column
        if let Some(where_expr) = where_clause {
            return expression_references_column(where_expr, &pk_col_lower);
        }

        false
    }
}

/// Check if an expression references a specific column (case-insensitive)
fn expression_references_column(expr: &Expression, column: &str) -> bool {
    match expr {
        Expression::ColumnRef(col_id) => col_id.column_canonical().to_lowercase() == column,
        Expression::BinaryOp { left, right, .. } => {
            expression_references_column(left, column)
                || expression_references_column(right, column)
        }
        Expression::Conjunction(exprs) | Expression::Disjunction(exprs) => {
            exprs.iter().any(|e| expression_references_column(e, column))
        }
        Expression::UnaryOp { expr: inner, .. } => expression_references_column(inner, column),
        Expression::IsNull { expr: inner, .. } => expression_references_column(inner, column),
        Expression::Between { expr, low, high, .. } => {
            expression_references_column(expr, column)
                || expression_references_column(low, column)
                || expression_references_column(high, column)
        }
        Expression::InList { expr, values, .. } => {
            expression_references_column(expr, column)
                || values.iter().any(|e| expression_references_column(e, column))
        }
        _ => false,
    }
}

/// Extract column references from a SELECT list for covering index detection
fn extract_select_columns(select_list: &[SelectItem]) -> HashSet<String> {
    let mut columns = HashSet::new();
    for item in select_list {
        match item {
            SelectItem::Wildcard { .. } => {
                // Wildcard means all columns needed - can't be a covering index
                // Return empty set with a special marker
                return HashSet::new();
            }
            SelectItem::QualifiedWildcard { .. } => {
                // table.* also means all columns from that table
                return HashSet::new();
            }
            SelectItem::Expression { expr, .. } => {
                collect_column_refs(expr, &mut columns);
            }
        }
    }
    columns
}

/// Recursively collect column references from an expression
fn collect_column_refs(expr: &Expression, columns: &mut HashSet<String>) {
    match expr {
        Expression::ColumnRef(col_id) => {
            columns.insert(col_id.column_canonical().to_lowercase());
        }
        Expression::BinaryOp { left, right, .. } => {
            collect_column_refs(left, columns);
            collect_column_refs(right, columns);
        }
        Expression::UnaryOp { expr: inner, .. } => {
            collect_column_refs(inner, columns);
        }
        Expression::Conjunction(exprs) | Expression::Disjunction(exprs) => {
            for e in exprs {
                collect_column_refs(e, columns);
            }
        }
        Expression::Function { args, .. } | Expression::AggregateFunction { args, .. } => {
            for arg in args {
                collect_column_refs(arg, columns);
            }
        }
        Expression::IsNull { expr: inner, .. } => {
            collect_column_refs(inner, columns);
        }
        Expression::Between { expr, low, high, .. } => {
            collect_column_refs(expr, columns);
            collect_column_refs(low, columns);
            collect_column_refs(high, columns);
        }
        Expression::InList { expr, values, .. } => {
            collect_column_refs(expr, columns);
            for v in values {
                collect_column_refs(v, columns);
            }
        }
        Expression::Case { operand, when_clauses, else_result, .. } => {
            if let Some(op) = operand {
                collect_column_refs(op, columns);
            }
            for case_when in when_clauses {
                for cond in &case_when.conditions {
                    collect_column_refs(cond, columns);
                }
                collect_column_refs(&case_when.result, columns);
            }
            if let Some(else_res) = else_result {
                collect_column_refs(else_res, columns);
            }
        }
        Expression::Cast { expr: inner, .. } => {
            collect_column_refs(inner, columns);
        }
        Expression::ScalarSubquery(_)
        | Expression::In { .. }
        | Expression::Literal(_)
        | Expression::Placeholder(_)
        | Expression::NumberedPlaceholder(_)
        | Expression::NamedPlaceholder(_)
        | Expression::Wildcard => {}
        _ => {}  // Handle any other variants
    }
}

/// Check if all needed columns are covered by an index (covering index)
fn is_covering_index(
    index_name: &str,
    needed_columns: &HashSet<String>,
    database: &Database,
) -> bool {
    // If we need all columns (wildcard was used), not a covering index
    if needed_columns.is_empty() {
        return false;
    }

    let Some(index) = database.get_index(index_name) else {
        return false;
    };

    // Get index columns
    let index_columns: HashSet<String> = index
        .columns
        .iter()
        .map(|c| c.expect_column_name().to_lowercase())
        .collect();

    // Check if all needed columns are in the index
    needed_columns.iter().all(|col| index_columns.contains(col))
}

/// Extract index predicates from a WHERE clause for SQLite EQP format
///
/// Returns a list of (column_name, operator) tuples for predicates that can
/// use the index. The operator is one of "=", ">", "<", ">=", "<=".
/// Predicates are sorted by their position in the index column order.
fn extract_index_predicates(
    expr: &Expression,
    index_name: &str,
    database: &Database,
) -> Vec<(String, String)> {
    let mut predicates = Vec::new();

    // Get the index columns
    let index_columns: Vec<String> = database
        .get_index(index_name)
        .map(|idx| idx.columns.iter().map(|c| c.expect_column_name().to_lowercase()).collect())
        .unwrap_or_default();

    extract_predicates_recursive(expr, &index_columns, &mut predicates);

    // Sort predicates by their position in the index column order
    predicates.sort_by_key(|(col, _)| {
        let col_lower = col.to_lowercase();
        index_columns.iter().position(|c| c == &col_lower).unwrap_or(usize::MAX)
    });

    predicates
}

fn extract_predicates_recursive(
    expr: &Expression,
    index_columns: &[String],
    predicates: &mut Vec<(String, String)>,
) {
    use vibesql_ast::BinaryOperator;

    match expr {
        Expression::BinaryOp { op, left, right } => {
            // Check if this is a comparison operator
            let op_str = match op {
                BinaryOperator::Equal => Some("="),
                BinaryOperator::NotEqual => None, // Don't use for index
                BinaryOperator::LessThan => Some("<"),
                BinaryOperator::LessThanOrEqual => Some("<="),
                BinaryOperator::GreaterThan => Some(">"),
                BinaryOperator::GreaterThanOrEqual => Some(">="),
                BinaryOperator::And => {
                    // Recurse for AND (but we typically have Conjunction)
                    extract_predicates_recursive(left, index_columns, predicates);
                    extract_predicates_recursive(right, index_columns, predicates);
                    return;
                }
                _ => None,
            };

            if let Some(op_str) = op_str {
                // Check if left side is a column reference that matches index columns
                if let Expression::ColumnRef(col_id) = left.as_ref() {
                    let col_name = col_id.column_canonical().to_lowercase();
                    if index_columns.iter().any(|c| c == &col_name) {
                        predicates
                            .push((col_id.column_canonical().to_string(), op_str.to_string()));
                    }
                }
                // Check if right side is a column reference (for reversed comparisons)
                else if let Expression::ColumnRef(col_id) = right.as_ref() {
                    let col_name = col_id.column_canonical().to_lowercase();
                    if index_columns.iter().any(|c| c == &col_name) {
                        // Reverse the operator for column on right side
                        let reversed_op = match op {
                            BinaryOperator::Equal => "=",
                            BinaryOperator::LessThan => ">",
                            BinaryOperator::LessThanOrEqual => ">=",
                            BinaryOperator::GreaterThan => "<",
                            BinaryOperator::GreaterThanOrEqual => "<=",
                            _ => return,
                        };
                        predicates
                            .push((col_id.column_canonical().to_string(), reversed_op.to_string()));
                    }
                }
            }
        }
        Expression::Conjunction(exprs) => {
            for e in exprs {
                extract_predicates_recursive(e, index_columns, predicates);
            }
        }
        Expression::Between { expr, negated: false, .. } => {
            // BETWEEN is equivalent to >= AND <=
            if let Expression::ColumnRef(col_id) = expr.as_ref() {
                let col_name = col_id.column_canonical().to_lowercase();
                if index_columns.iter().any(|c| c == &col_name) {
                    let col = col_id.column_canonical().to_string();
                    predicates.push((col.clone(), ">=".to_string()));
                    predicates.push((col, "<=".to_string()));
                }
            }
        }
        Expression::InList { expr, negated: false, .. } => {
            // IN list treated as equality
            if let Expression::ColumnRef(col_id) = expr.as_ref() {
                let col_name = col_id.column_canonical().to_lowercase();
                if index_columns.iter().any(|c| c == &col_name) {
                    predicates.push((col_id.column_canonical().to_string(), "=".to_string()));
                }
            }
        }
        _ => {}
    }
}
