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
    errors::ExecutorError,
    optimizer::index_planner::IndexPlanner,
    select::scan::index_scan::{
        cost_based_index_selection, eqp_ordering_index, needs_temp_btree_for_order_by_eqp,
    },
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
    /// Whether this query requires a temp B-tree for ORDER BY
    pub needs_temp_btree_for_order_by: bool,
    /// Number of distinct window-function sorting passes (PARTITION BY +
    /// ORDER BY keys) not satisfied by an index. Each contributes one
    /// `USE TEMP B-TREE FOR ORDER BY` entry in EQP output.
    pub window_sort_count: usize,
    /// Set operation type for compound queries (UNION, INTERSECT, EXCEPT)
    pub set_operation_type: Option<String>,
    /// Whether this is a compound query root
    pub is_compound_query: bool,
    /// When set, this node is a window-function subquery/view rendered as a
    /// SQLite-style `CO-ROUTINE <name>` block in EQP output: the inner plan
    /// is nested under the CO-ROUTINE entry and the outer query reads it via
    /// a trailing `SCAN <name>` entry (windowpushd.test, #5347).
    pub coroutine: Option<String>,
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
            needs_temp_btree_for_order_by: false,
            window_sort_count: 0,
            set_operation_type: None,
            is_compound_query: false,
            coroutine: None,
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
    /// QUERY PLAN
    /// |--SCAN t1 USING INDEX i1
    /// `--USE TEMP B-TREE FOR LAST TERM OF ORDER BY
    /// ```
    ///
    /// For compound queries:
    /// ```text
    /// QUERY PLAN
    /// `--COMPOUND QUERY
    ///    |--LEFT-MOST SUBQUERY
    ///    |  `--SCAN t1
    ///    `--UNION ALL
    ///       `--SCAN t2
    /// ```
    ///
    /// The "QUERY PLAN" header is included to match SQLite's EQP format used in
    /// TCL tests with full format comparison (do_eqp_test with QUERY PLAN prefix).
    pub fn to_sqlite_eqp(&self) -> String {
        let mut output = String::new();

        // Add "QUERY PLAN" header to match SQLite's EQP format
        writeln!(output, "QUERY PLAN").unwrap();

        // Collect all EQP entries (scan nodes, CO-ROUTINE blocks, COMPOUND
        // QUERY blocks, temp b-tree entries) and render the tree.
        let entries = collect_eqp_entries(&self.plan);
        write_eqp_entries(&entries, "", &mut output);

        output
    }

    /// Format the plan as SQLite-compatible EXPLAIN output (VM bytecode style)
    ///
    /// This produces output mimicking SQLite's VDBE bytecode format:
    /// ```text
    /// addr  opcode         p1    p2    p3    p4             p5  comment
    /// ----  -------------  ----  ----  ----  -------------  --  -------------
    /// 0     Init           0     8     0                    0   Start at 8
    /// 1     OpenRead       0     2     0     2              0   root=2 iDb=0; t1
    /// ...
    /// ```
    ///
    /// Note: This is a synthetic representation since VibeSQL doesn't use SQLite's VM.
    /// The opcodes are generated to approximate what SQLite would produce.
    pub fn to_sqlite_vm(&self) -> SqliteVmOutput {
        let mut instructions = Vec::new();
        let mut addr = 0;

        // Collect scan nodes to determine table access patterns
        let scan_nodes = collect_scan_nodes(&self.plan);

        // Generate Init instruction (always first)
        // p2 will be updated to point to Transaction after we know total instructions
        let init_addr = addr;
        instructions.push(VmInstruction {
            addr,
            opcode: "Init".to_string(),
            p1: 0,
            p2: 0, // Will be patched later
            p3: 0,
            p4: String::new(),
            p5: 0,
            comment: "Start at ?".to_string(),
        });
        addr += 1;

        // Generate OpenRead for each table
        let mut cursor = 0;
        let mut table_cursors = std::collections::HashMap::new();

        for node in &scan_nodes {
            if let Some(ref table_name) = node.object {
                if !table_cursors.contains_key(table_name) {
                    let root_page = 2 + cursor; // Synthetic root page
                    instructions.push(VmInstruction {
                        addr,
                        opcode: "OpenRead".to_string(),
                        p1: cursor,
                        p2: root_page,
                        p3: 0,
                        p4: "2".to_string(), // Number of columns (synthetic)
                        p5: 0,
                        comment: format!("root={} iDb=0; {}", root_page, table_name),
                    });
                    table_cursors.insert(table_name.clone(), cursor);
                    cursor += 1;
                    addr += 1;
                }
            }
        }

        // Generate seek/scan instructions based on scan type
        let result_row_addr = addr + scan_nodes.len() * 2 + 1;
        let halt_addr = result_row_addr + 1;

        for node in &scan_nodes {
            if let Some(ref table_name) = node.object {
                let cursor_id = *table_cursors.get(table_name).unwrap_or(&0);

                match node.scan_type.as_ref() {
                    Some(ScanType::Search) | Some(ScanType::CoveringIndex) => {
                        // Index seek
                        instructions.push(VmInstruction {
                            addr,
                            opcode: "SeekGE".to_string(),
                            p1: cursor_id,
                            p2: halt_addr as i32,
                            p3: 1,
                            p4: String::new(),
                            p5: 0,
                            comment: format!("key=r[1]; {}", table_name),
                        });
                        addr += 1;
                    }
                    Some(ScanType::IntegerPrimaryKey) => {
                        // Primary key lookup
                        instructions.push(VmInstruction {
                            addr,
                            opcode: "SeekRowid".to_string(),
                            p1: cursor_id,
                            p2: halt_addr as i32,
                            p3: 1,
                            p4: String::new(),
                            p5: 0,
                            comment: format!("pk; {}", table_name),
                        });
                        addr += 1;
                    }
                    Some(ScanType::Scan) | None => {
                        // Full table scan - use Rewind
                        instructions.push(VmInstruction {
                            addr,
                            opcode: "Rewind".to_string(),
                            p1: cursor_id,
                            p2: halt_addr as i32,
                            p3: 0,
                            p4: String::new(),
                            p5: 0,
                            comment: table_name.clone(),
                        });
                        addr += 1;
                    }
                }
            }
        }

        // Generate Column and ResultRow instructions
        let mut register = 1;
        for (i, node) in scan_nodes.iter().enumerate() {
            if let Some(ref table_name) = node.object {
                let cursor_id = *table_cursors.get(table_name).unwrap_or(&0);
                // Column instruction for each output column
                instructions.push(VmInstruction {
                    addr,
                    opcode: "Column".to_string(),
                    p1: cursor_id,
                    p2: i as i32, // Column index
                    p3: register,
                    p4: String::new(),
                    p5: 0,
                    comment: format!("r[{}]=cursor {} column {}", register, cursor_id, i),
                });
                register += 1;
                addr += 1;
            }
        }

        // ResultRow instruction
        instructions.push(VmInstruction {
            addr,
            opcode: "ResultRow".to_string(),
            p1: 1,
            p2: register - 1,
            p3: 0,
            p4: String::new(),
            p5: 0,
            comment: format!("output=r[1..{}]", register - 1),
        });
        addr += 1;

        // Next/Goto for each scan
        for node in &scan_nodes {
            if let Some(ref table_name) = node.object {
                let cursor_id = *table_cursors.get(table_name).unwrap_or(&0);
                let seek_addr = scan_nodes
                    .iter()
                    .position(|n| n.object.as_ref() == Some(table_name))
                    .map(|pos| 1 + table_cursors.len() + pos)
                    .unwrap_or(1);

                instructions.push(VmInstruction {
                    addr,
                    opcode: "Next".to_string(),
                    p1: cursor_id,
                    p2: seek_addr as i32,
                    p3: 0,
                    p4: String::new(),
                    p5: 0,
                    comment: table_name.clone(),
                });
                addr += 1;
            }
        }

        // Halt instruction
        instructions.push(VmInstruction {
            addr,
            opcode: "Halt".to_string(),
            p1: 0,
            p2: 0,
            p3: 0,
            p4: String::new(),
            p5: 0,
            comment: String::new(),
        });
        addr += 1;

        // Transaction instruction (usually near the end in SQLite)
        let transaction_addr = addr;
        instructions.push(VmInstruction {
            addr,
            opcode: "Transaction".to_string(),
            p1: 0,
            p2: 0,
            p3: 1,
            p4: "0".to_string(),
            p5: 1,
            comment: "usesStmtJournal=0".to_string(),
        });
        addr += 1;

        // Goto to first instruction after Init
        instructions.push(VmInstruction {
            addr,
            opcode: "Goto".to_string(),
            p1: 0,
            p2: 1,
            p3: 0,
            p4: String::new(),
            p5: 0,
            comment: String::new(),
        });

        // Patch Init instruction to jump to Transaction
        if let Some(init) = instructions.get_mut(init_addr) {
            init.p2 = transaction_addr as i32;
            init.comment = format!("Start at {}", transaction_addr);
        }

        SqliteVmOutput { instructions }
    }
}

/// Represents a single SQLite VM instruction for EXPLAIN output
#[derive(Debug, Clone)]
pub struct VmInstruction {
    /// Instruction address (sequential)
    pub addr: usize,
    /// Opcode name (e.g., "OpenRead", "SeekGE", "Column")
    pub opcode: String,
    /// First integer parameter
    pub p1: i32,
    /// Second integer parameter
    pub p2: i32,
    /// Third integer parameter
    pub p3: i32,
    /// String parameter
    pub p4: String,
    /// Fifth integer parameter (flags)
    pub p5: i32,
    /// Human-readable comment
    pub comment: String,
}

/// SQLite VM EXPLAIN output
#[derive(Debug)]
pub struct SqliteVmOutput {
    /// List of VM instructions
    pub instructions: Vec<VmInstruction>,
}

impl SqliteVmOutput {
    /// Get the column names for SQLite EXPLAIN output
    pub fn column_names() -> Vec<&'static str> {
        vec!["addr", "opcode", "p1", "p2", "p3", "p4", "p5", "comment"]
    }

    /// Convert to rows for display
    pub fn to_rows(&self) -> Vec<Vec<String>> {
        self.instructions
            .iter()
            .map(|inst| {
                vec![
                    inst.addr.to_string(),
                    inst.opcode.clone(),
                    inst.p1.to_string(),
                    inst.p2.to_string(),
                    inst.p3.to_string(),
                    inst.p4.clone(),
                    inst.p5.to_string(),
                    inst.comment.clone(),
                ]
            })
            .collect()
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

/// A single EQP output entry with nested children (used for SQLite-style
/// `CO-ROUTINE <name>` blocks whose inner plan renders indented).
struct EqpEntry {
    text: String,
    children: Vec<EqpEntry>,
}

impl EqpEntry {
    fn leaf(text: String) -> Self {
        EqpEntry { text, children: Vec::new() }
    }
}

/// Render a list of EQP entries as SQLite's tree format with `|--`/`` `-- ``
/// connectors and `|  `/three-space child indentation.
fn write_eqp_entries(entries: &[EqpEntry], indent: &str, output: &mut String) {
    for (i, entry) in entries.iter().enumerate() {
        let is_last = i == entries.len() - 1;
        let connector = if is_last { "`--" } else { "|--" };
        writeln!(output, "{}{}{}", indent, connector, entry.text).unwrap();
        if !entry.children.is_empty() {
            let child_indent = format!("{}{}", indent, if is_last { "   " } else { "|  " });
            write_eqp_entries(&entry.children, &child_indent, output);
        }
    }
}

/// Collect scan entries from the plan tree. Window-subquery/view nodes
/// marked as co-routines render as a `CO-ROUTINE <name>` block containing
/// the inner plan's entries (including the inner query's window-sort temp
/// B-tree entries), followed by the outer query's `SCAN <name>` of the
/// co-routine output — matching SQLite's EQP shape for window views
/// (windowpushd.test, #5347).
fn append_scan_entries(node: &PlanNode, entries: &mut Vec<EqpEntry>) {
    if let Some(ref name) = node.coroutine {
        let mut children = Vec::new();
        for child in &node.children {
            children.extend(collect_eqp_entries(child));
        }
        entries.push(EqpEntry { text: format!("CO-ROUTINE {}", name), children });
        entries.push(EqpEntry::leaf(format!("SCAN {}", name)));
        return;
    }

    // Compound (UNION/INTERSECT/EXCEPT) roots render as a nested
    // `COMPOUND QUERY` block. This both drives the top-level rendering in
    // `to_sqlite_eqp` and lets compound view/subquery bodies nest inside
    // `CO-ROUTINE` blocks (#5361), matching SQLite's shape:
    //   |--CO-ROUTINE cv
    //   |  `--COMPOUND QUERY
    //   |     |--LEFT-MOST SUBQUERY
    //   ...
    if node.is_compound_query {
        entries.push(compound_eqp_entry(node));
        return;
    }

    if node.scan_type.is_some() {
        entries.push(EqpEntry::leaf(format_sqlite_eqp_node(node)));
    }

    for child in &node.children {
        append_scan_entries(child, entries);
    }
}

/// Build the `COMPOUND QUERY` entry tree for a compound-query plan root:
/// a `LEFT-MOST SUBQUERY` child for the first branch, then one child per
/// set operation (`UNION`, `UNION ALL`, `INTERSECT`, `EXCEPT`, ...), each
/// containing that branch's scan entries.
fn compound_eqp_entry(node: &PlanNode) -> EqpEntry {
    let mut children = Vec::new();
    for (i, branch) in node.children.iter().enumerate() {
        let label = if i == 0 {
            "LEFT-MOST SUBQUERY".to_string()
        } else {
            branch.set_operation_type.clone().unwrap_or_else(|| "UNION ALL".to_string())
        };
        let scans = collect_scan_nodes(branch)
            .into_iter()
            .map(|scan| EqpEntry::leaf(format_sqlite_eqp_node(scan)))
            .collect();
        children.push(EqpEntry { text: label, children: scans });
    }
    EqpEntry { text: "COMPOUND QUERY".to_string(), children }
}

/// True when any node in `node`'s subtree needs a temp B-tree for ORDER BY.
///
/// Used by the view-flattening branch to hoist a body's sort flag onto the
/// `Subquery` node it nests under, since [`collect_eqp_entries`] only checks
/// a node and its DIRECT children. Co-routine subtrees are skipped: their
/// inner entries (including temp B-tree lines) are re-collected inside the
/// `CO-ROUTINE` block by [`append_scan_entries`], so counting them here
/// would emit the line twice.
fn subtree_needs_order_by_temp_btree(node: &PlanNode) -> bool {
    if node.coroutine.is_some() {
        return false;
    }
    node.needs_temp_btree_for_order_by
        || node.children.iter().any(subtree_needs_order_by_temp_btree)
}

/// Collect all EQP entries from the plan tree, including TEMP B-TREE entries
fn collect_eqp_entries(node: &PlanNode) -> Vec<EqpEntry> {
    let mut entries = Vec::new();

    // Collect scan entries first (co-routine blocks nest their inner plan)
    append_scan_entries(node, &mut entries);

    // Add one TEMP B-TREE entry per distinct window sort key not satisfied
    // by an index. Window sorting passes run before the statement-level
    // ORDER BY, and SQLite never dedups them against it — they are
    // separate passes.
    for _ in 0..node.window_sort_count {
        entries.push(EqpEntry::leaf("USE TEMP B-TREE FOR ORDER BY".to_string()));
    }

    // Add TEMP B-TREE entry if needed for ORDER BY
    if node.needs_temp_btree_for_order_by {
        entries.push(EqpEntry::leaf("USE TEMP B-TREE FOR ORDER BY".to_string()));
    }

    // Check children for temp b-tree needs as well
    for child in &node.children {
        if child.needs_temp_btree_for_order_by && !node.needs_temp_btree_for_order_by {
            entries.push(EqpEntry::leaf("USE TEMP B-TREE FOR ORDER BY".to_string()));
            break;
        }
    }

    entries
}

/// Format a single node in SQLite EQP style
fn format_sqlite_eqp_node(node: &PlanNode) -> String {
    // Handle constant row scan (no FROM clause / VALUES), e.g.
    // `SCAN CONSTANT ROW` or `SCAN 2 CONSTANT ROWS`.
    if node.operation == "SCAN CONSTANT ROW" || node.operation.ends_with("CONSTANT ROWS") {
        return node.operation.clone();
    }

    let table_name = node.object.as_deref().unwrap_or("?");

    match node.scan_type.as_ref() {
        Some(ScanType::Scan) => {
            // Check if we have an index for ordering (SCAN table USING INDEX idx)
            if let Some(ref index_name) = node.index_name {
                format!("SCAN {} USING INDEX {}", table_name, index_name)
            } else {
                format!("SCAN {}", table_name)
            }
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
            Statement::Select(select_stmt) => {
                Self::explain_select(select_stmt, database, &HashSet::new())?
            }
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
    ///
    /// `outer_ctes` holds the lowercased names of CTEs in scope from
    /// enclosing queries; CTE names shadow same-named catalog views, so the
    /// window push-down rewrite and view expansion must not fire for them.
    fn explain_select(
        stmt: &SelectStmt,
        database: &Database,
        outer_ctes: &HashSet<String>,
    ) -> Result<PlanNode, ExecutorError> {
        // Mirror the runtime optimizer (#5292): WHERE conjuncts on a
        // PARTITION BY prefix of every window are pushed into window-function
        // views/subqueries before planning, so EQP reflects the inner access
        // path (windowpushd.test 1.4, 2.1.*, #5347). When the pass does not
        // fire the statement is returned unchanged.
        let stmt =
            crate::optimizer::push_where_into_window_subqueries(stmt.clone(), database, outer_ctes);
        let stmt = &stmt;

        // Extend the CTE scope with this statement's own WITH clause for
        // nested FROM-clause analysis.
        let mut ctes = outer_ctes.clone();
        if let Some(ref with) = stmt.with_clause {
            for cte in with.iter() {
                ctes.insert(cte.name.to_ascii_lowercase());
            }
        }

        // Check if this is a compound query (UNION, INTERSECT, EXCEPT)
        if stmt.set_operation.is_some() {
            return Self::explain_compound_select(stmt, database, &ctes);
        }

        let mut root = PlanNode::new("Select");

        // Extract columns needed by the SELECT list for covering index detection
        let needed_columns = extract_select_columns(&stmt.select_list);

        // Analyze FROM clause. When the SELECT list contains window
        // functions, the base-table scan feeds the INNERMOST window sorting
        // pass (SQLite's co-routine rewrite, see count_window_sorts), so the
        // scan's effective ordering requirement is the last distinct window
        // key — SQLite picks an index that delivers PARTITION BY/ORDER BY
        // order even without any predicate (windowpushd.test 2.1.3.6).
        let window_scan_key = Self::distinct_window_keys(stmt).pop();
        let order_from_window = window_scan_key.is_some();
        let scan_order_by: Option<Vec<vibesql_ast::OrderByItem>> =
            window_scan_key.or_else(|| stmt.order_by.clone());
        if let Some(ref from_clause) = stmt.from {
            let scan_node = Self::explain_from_clause(
                from_clause,
                &stmt.where_clause,
                &scan_order_by,
                order_from_window,
                &needed_columns,
                database,
                &ctes,
            )?;
            root.add_child(scan_node);
        } else {
            // No FROM clause - this is a constant expression scan. Multi-row
            // VALUES bodies render SQLite's plural `SCAN <n> CONSTANT ROWS`;
            // a single row (or a plain FROM-less SELECT) keeps the singular
            // `SCAN CONSTANT ROW` (verified against sqlite3 3.51.0, #5361).
            let mut constant_node = PlanNode::new("Constant Row");
            constant_node.scan_type = Some(ScanType::Scan);
            constant_node.operation = match &stmt.values {
                Some(rows) if rows.len() > 1 => format!("SCAN {} CONSTANT ROWS", rows.len()),
                _ => "SCAN CONSTANT ROW".to_string(),
            };
            root.add_child(constant_node);
        }

        // Check if we need a temp B-tree for ORDER BY
        // This happens when ORDER BY cannot be satisfied by an index. The WHERE
        // clause is passed through so the planner can pin leading index columns
        // and accept trailing ORDER BY columns that would otherwise be rejected
        // due to nullability.
        //
        // The entry is also suppressed when the statement-level ORDER BY is a
        // structural prefix of (or equal to) the FIRST SELECT-list window's
        // combined sort key. SQLite's window co-routine rewrite places the
        // first window's sorting pass outermost, so its key is the final
        // output order and the outer sort is satisfied without a second temp
        // B-tree (verified against sqlite3 3.51.0). An empty key (`OVER ()`)
        // never suppresses; extensions, direction/COLLATE mismatches, and
        // matches against later windows do not suppress.
        if let Some(ref order_by) = stmt.order_by {
            let satisfied_by_window_sort =
                Self::first_window_combined_key(stmt).is_some_and(|key| {
                    !key.is_empty()
                        && order_by.len() <= key.len()
                        && key[..order_by.len()] == order_by[..]
                });
            root.needs_temp_btree_for_order_by = !satisfied_by_window_sort
                && Self::needs_temp_btree_for_order_by(
                    stmt.from.as_ref(),
                    stmt.where_clause.as_ref(),
                    order_by,
                    database,
                );
        }

        // Count window-function sorting passes for EQP output. SQLite emits
        // one "USE TEMP B-TREE FOR ORDER BY" entry per distinct window sort
        // key not satisfied by an index (window1.test section 23).
        root.window_sort_count = Self::count_window_sorts(stmt, database);

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

    /// Generate execution plan for a compound SELECT (UNION, INTERSECT, EXCEPT)
    fn explain_compound_select(
        stmt: &SelectStmt,
        database: &Database,
        ctes: &HashSet<String>,
    ) -> Result<PlanNode, ExecutorError> {
        let mut root = PlanNode::new("CompoundQuery");
        root.is_compound_query = true;

        // Add left-most subquery
        let left_stmt = SelectStmt {
            with_clause: stmt.with_clause.clone(),
            distinct: stmt.distinct,
            select_list: stmt.select_list.clone(),
            into_table: stmt.into_table.clone(),
            into_variables: stmt.into_variables.clone(),
            from: stmt.from.clone(),
            where_clause: stmt.where_clause.clone(),
            group_by: stmt.group_by.clone(),
            having: stmt.having.clone(),
            window_definitions: stmt.window_definitions.clone(),
            order_by: None, // ORDER BY applies to the compound result
            limit: None,
            offset: None,
            set_operation: None,
            values: stmt.values.clone(),
        };
        let left_plan = Self::explain_select(&left_stmt, database, ctes)?;
        root.add_child(left_plan);

        // Add subsequent set operations
        let mut current_set_op = stmt.set_operation.as_ref();
        while let Some(set_op) = current_set_op {
            let op_label = match (&set_op.op, set_op.all) {
                (vibesql_ast::SetOperator::Union, true) => "UNION ALL",
                (vibesql_ast::SetOperator::Union, false) => "UNION",
                (vibesql_ast::SetOperator::Intersect, true) => "INTERSECT ALL",
                (vibesql_ast::SetOperator::Intersect, false) => "INTERSECT",
                (vibesql_ast::SetOperator::Except, true) => "EXCEPT ALL",
                (vibesql_ast::SetOperator::Except, false) => "EXCEPT",
            };

            // Create plan for right side of set operation
            let right_stmt = SelectStmt {
                with_clause: set_op.right.with_clause.clone(),
                distinct: set_op.right.distinct,
                select_list: set_op.right.select_list.clone(),
                into_table: set_op.right.into_table.clone(),
                into_variables: set_op.right.into_variables.clone(),
                from: set_op.right.from.clone(),
                where_clause: set_op.right.where_clause.clone(),
                group_by: set_op.right.group_by.clone(),
                having: set_op.right.having.clone(),
                window_definitions: set_op.right.window_definitions.clone(),
                order_by: None,
                limit: None,
                offset: None,
                set_operation: None,
                values: set_op.right.values.clone(),
            };
            let mut right_plan = Self::explain_select(&right_stmt, database, ctes)?;
            right_plan.set_operation_type = Some(op_label.to_string());
            root.add_child(right_plan);

            // Continue to nested set operations
            current_set_op = set_op.right.set_operation.as_ref();
        }

        Ok(root)
    }

    /// Count the distinct window-function sorting passes required by the
    /// SELECT list that are not satisfied by an index.
    ///
    /// SQLite semantics (window1.test section 23, `do_ordercount_test`):
    /// - The sort key for a window is its PARTITION BY expressions (treated
    ///   as ASC with default null ordering) followed by its ORDER BY items.
    ///   `OVER (PARTITION BY a ORDER BY b)` and `OVER (ORDER BY a, b)` share
    ///   the key `(a, b)`.
    /// - Keys are deduplicated by exact structural equality (including
    ///   direction and COLLATE); frame clauses are ignored entirely.
    /// - Windows with neither PARTITION BY nor ORDER BY (`OVER ()`) need no
    ///   sorting pass.
    /// - Only the INNERMOST sorting pass scans the base table and can be
    ///   satisfied by an index (e.g. key `(a, b)` with index `t5ab(a, b)`).
    ///   SQLite's nested co-routine rewrite emits passes in reverse
    ///   SELECT-list order, so the innermost pass corresponds to the LAST
    ///   distinct key (by first occurrence).
    /// - When the innermost pass IS satisfied by an index, sortedness
    ///   propagates outward: each outer pass whose key is a structural
    ///   prefix of the order delivered by the pass below it needs no sort
    ///   either, because the rows flow through in an order that already
    ///   satisfies it (window9.test 5.1.1: index `i1(a,b,c,d,e)` satisfies
    ///   keys `(a,b,c,d)`, `(a,b,c)`, `(a,b)` and `(a)` — zero sorts).
    ///   The chain breaks at the first outer key that is not such a prefix;
    ///   that key and all keys outside it need temp B-trees (the temp
    ///   B-tree sort is not guaranteed to preserve residual ordering, so no
    ///   further propagation is attempted — matching the conservative
    ///   pre-existing behavior validated by window1.test section 23).
    fn count_window_sorts(stmt: &SelectStmt, database: &Database) -> usize {
        let distinct_keys = Self::distinct_window_keys(stmt);

        // Only the innermost pass — the last distinct key — scans the base
        // table; it alone is eligible for direct index suppression. When it
        // is satisfied by the index, sortedness propagates outward to every
        // consecutive outer key that is a structural prefix of the order
        // delivered by the pass below it. Once a pass needs a temp B-tree,
        // all passes outside it count as well.
        let Some((innermost, outer)) = distinct_keys.split_last() else {
            return 0;
        };

        let mut count = 0;
        // The order delivered to outer passes while the suppression chain
        // is unbroken. `None` once any pass has required a temp B-tree.
        let mut delivered: Option<&[vibesql_ast::OrderByItem]> =
            if Self::window_key_satisfied_by_index(
                stmt.from.as_ref(),
                stmt.where_clause.as_ref(),
                innermost,
                database,
            ) {
                Some(innermost.as_slice())
            } else {
                count += 1;
                None
            };

        // Walk outward (last-but-one key back to the first).
        for key in outer.iter().rev() {
            match delivered {
                Some(order)
                    if key.len() <= order.len() && key.as_slice() == &order[..key.len()] =>
                {
                    // Structural prefix of the incoming order — rows flow
                    // through already sorted; no temp B-tree needed. The
                    // delivered order is unchanged.
                }
                _ => {
                    count += 1;
                    delivered = None;
                }
            }
        }

        count
    }

    /// The distinct window sort keys of the SELECT list, in first-occurrence
    /// order. Each key is PARTITION BY exprs (as ASC) + window ORDER BY
    /// items; empty keys (`OVER ()`) are skipped. SQLite's nested co-routine
    /// rewrite emits sorting passes in reverse order, so the LAST key is the
    /// innermost pass — the one that scans the base table. Resolution errors
    /// (e.g. unknown named window) surface during execution; EXPLAIN just
    /// sees no window keys.
    fn distinct_window_keys(stmt: &SelectStmt) -> Vec<Vec<vibesql_ast::OrderByItem>> {
        let Ok(specs) = crate::select::window::collect_resolved_window_specs(
            &stmt.select_list,
            stmt.window_definitions.as_ref(),
        ) else {
            return Vec::new();
        };

        let mut distinct_keys: Vec<Vec<vibesql_ast::OrderByItem>> = Vec::new();
        for spec in &specs {
            let key = Self::window_combined_key(spec);

            // OVER () — no partitioning or ordering — needs no sort pass.
            if key.is_empty() {
                continue;
            }

            if !distinct_keys.contains(&key) {
                distinct_keys.push(key);
            }
        }
        distinct_keys
    }

    /// Build the combined sort key for a window spec: PARTITION BY
    /// expressions (treated as ASC with default null ordering) followed by
    /// the window's ORDER BY items. `OVER ()` yields an empty key.
    fn window_combined_key(spec: &vibesql_ast::WindowSpec) -> Vec<vibesql_ast::OrderByItem> {
        let mut key: Vec<vibesql_ast::OrderByItem> = Vec::new();
        if let Some(partition_by) = &spec.partition_by {
            for expr in partition_by {
                key.push(vibesql_ast::OrderByItem {
                    expr: expr.clone(),
                    direction: vibesql_ast::OrderDirection::Asc,
                    nulls_order: None,
                });
            }
        }
        if let Some(order_by) = &spec.order_by {
            key.extend(order_by.iter().cloned());
        }
        key
    }

    /// Combined sort key of the FIRST window function in the SELECT list, if
    /// any. SELECT-list order is preserved by
    /// `collect_resolved_window_specs`, and SQLite's co-routine rewrite makes
    /// the first window's sorting pass the outermost one — its key is the
    /// order of the final output.
    fn first_window_combined_key(stmt: &SelectStmt) -> Option<Vec<vibesql_ast::OrderByItem>> {
        let specs = crate::select::window::collect_resolved_window_specs(
            &stmt.select_list,
            stmt.window_definitions.as_ref(),
        )
        .ok()?;
        specs.first().map(Self::window_combined_key)
    }

    /// EQP-level check: can the base-table scan deliver `key` order for the
    /// INNERMOST window sorting pass? Unlike the statement-level ORDER BY
    /// check, the window pass is fed directly by the FROM scan, and SQLite
    /// picks an index that delivers PARTITION BY/ORDER BY order even when no
    /// predicate can use it (windowpushd.test 2.1.1.5, 2.1.3.6) — hence
    /// `prefer_ordering_scan = true`.
    fn window_key_satisfied_by_index(
        from: Option<&vibesql_ast::FromClause>,
        where_clause: Option<&vibesql_ast::Expression>,
        key: &[vibesql_ast::OrderByItem],
        database: &Database,
    ) -> bool {
        let Some(from_clause) = from else {
            return true; // No FROM — single constant row, no sort needed.
        };
        let vibesql_ast::FromClause::Table { name, .. } = from_clause else {
            return false; // Joins/subqueries always need a sorting pass.
        };
        eqp_ordering_index(name, where_clause, key, database, true).is_some()
    }

    /// Check if ORDER BY requires a temp B-tree (sorting pass) for EQP rendering.
    ///
    /// Delegates to [`needs_temp_btree_for_order_by_eqp`] in the index-scan
    /// selection module, which threads the WHERE clause through and applies a
    /// permissive EQP-level check: when an index has its leading columns pinned
    /// by equality/IN predicates and the ORDER BY structurally aligns, no temp
    /// B-tree is shown — matching SQLite's behavior on plans like
    /// `WHERE a IN (1,2,3) ORDER BY a, b`. Runtime correctness for nullable
    /// trailing-column ordering is handled separately by the post-scan
    /// `apply_order_by` pass.
    fn needs_temp_btree_for_order_by(
        from: Option<&vibesql_ast::FromClause>,
        where_clause: Option<&vibesql_ast::Expression>,
        order_by: &[vibesql_ast::OrderByItem],
        database: &Database,
    ) -> bool {
        // If no FROM clause, this is a constant expression - no sorting needed
        // (e.g., "SELECT 5 ORDER BY 1" just returns one row)
        let Some(from_clause) = from else {
            return false;
        };

        // Get the table name
        let table_name = match from_clause {
            vibesql_ast::FromClause::Table { name, .. } => name.as_str(),
            _ => return true, // Joins/subqueries need temp B-tree
        };

        needs_temp_btree_for_order_by_eqp(table_name, where_clause, order_by, database)
    }

    /// True when the SELECT list (or named WINDOW clause) of `stmt` contains
    /// window functions — such subqueries/views render as CO-ROUTINE blocks
    /// in EQP output because SQLite cannot flatten them.
    fn select_has_window_functions(stmt: &SelectStmt) -> bool {
        crate::select::window::collect_resolved_window_specs(
            &stmt.select_list,
            stmt.window_definitions.as_ref(),
        )
        .map(|specs| !specs.is_empty())
        .unwrap_or(false)
    }

    /// True when a plain (window-free) view body is simple enough for its
    /// plan to be inlined into the outer EQP output (#5355).
    ///
    /// Mirrors SQLite's query-flattener blocking conditions conservatively:
    /// aggregates, GROUP BY/HAVING, DISTINCT, LIMIT/OFFSET, compound bodies,
    /// VALUES, and WITH clauses all render as `CO-ROUTINE <view>` blocks
    /// showing the body's inner plan (#5361) — truthful because the runtime
    /// materializes every view body. Window functions take the same
    /// CO-ROUTINE path (#5347).
    fn view_body_is_flattenable(stmt: &SelectStmt) -> bool {
        let select_list_has_aggregate = stmt.select_list.iter().any(|item| match item {
            SelectItem::Expression { expr, .. } => contains_aggregate_function(expr),
            SelectItem::Wildcard { .. } | SelectItem::QualifiedWildcard { .. } => false,
        });

        stmt.set_operation.is_none()
            && !stmt.distinct
            && stmt.group_by.is_none()
            && stmt.having.is_none()
            && stmt.limit.is_none()
            && stmt.offset.is_none()
            && stmt.values.is_none()
            && stmt.with_clause.is_none()
            && !select_list_has_aggregate
    }

    /// Generate plan node for FROM clause
    ///
    /// `order_from_window` is true when `order_by` is a window sort key
    /// rather than the statement-level ORDER BY; the base scan then prefers
    /// an index that delivers the window order even without predicates.
    fn explain_from_clause(
        from: &vibesql_ast::FromClause,
        where_clause: &Option<vibesql_ast::Expression>,
        order_by: &Option<Vec<vibesql_ast::OrderByItem>>,
        order_from_window: bool,
        needed_columns: &HashSet<String>,
        database: &Database,
        ctes: &HashSet<String>,
    ) -> Result<PlanNode, ExecutorError> {
        match from {
            vibesql_ast::FromClause::Table { name, alias, .. } => {
                // Expand views in EQP output instead of showing an opaque
                // `SCAN <view>`. CTE names shadow same-named views and are
                // never expanded.
                //
                // - Window-function views (#5347) and blocked bodies —
                //   aggregates, GROUP BY/HAVING, DISTINCT, LIMIT/OFFSET,
                //   compound, VALUES, WITH (#5361): SQLite cannot flatten
                //   them, so the body's plan renders as a CO-ROUTINE block
                //   plus a trailing `SCAN <name>` (windowpushd.test 2.1.3.6;
                //   sqlite3 3.51.0 verified per category). VibeSQL's runtime
                //   materializes every view body, so the block + inner plan
                //   is truthful even where SQLite manages to flatten a
                //   specific shape (LIMIT-only bodies, UNION ALL bodies,
                //   single-use plain CTEs — documented divergences in
                //   explain_view_expansion_tests.rs).
                // - Plain flattenable views (#5355): SQLite inlines the body
                //   into the outer query and shows the underlying table
                //   access with no mention of the view. VibeSQL's runtime
                //   MATERIALIZES views (select/scan/table.rs executes the
                //   full body, then post-filters the outer WHERE), so the
                //   inner scans shown here are the truthful access path; we
                //   deliberately do NOT fabricate SQLite's outer-WHERE
                //   push-down (`SEARCH <table> (x=?)`) because no index
                //   probe happens at runtime. Where no index applies the
                //   output matches SQLite exactly (`SCAN <table>`).
                if !ctes.contains(&name.to_ascii_lowercase()) {
                    if let Some(view) = database.catalog.get_view(name) {
                        let source = alias.as_deref().unwrap_or(name.as_str());
                        let child = Self::explain_select(&view.query, database, ctes)?;
                        let mut view_node = PlanNode::new("Subquery");
                        view_node.object = Some(format!("AS {}", source));

                        if Self::select_has_window_functions(&view.query)
                            || !Self::view_body_is_flattenable(&view.query)
                        {
                            // CO-ROUTINE block: the inner plan (including
                            // the body's own temp B-tree entries) renders
                            // inside the block via collect_eqp_entries, so
                            // no flag hoisting — subtree_needs_order_by_
                            // temp_btree skips co-routine subtrees to avoid
                            // double emission.
                            view_node.coroutine = Some(source.to_string());
                        } else {
                            // The body's ORDER BY genuinely sorts at runtime
                            // (views are materialized), and SQLite's
                            // flattened plan keeps the body's `USE TEMP
                            // B-TREE FOR ORDER BY` line. collect_eqp_entries
                            // only checks the root's DIRECT children for the
                            // flag, but the body root sits one level deeper
                            // (root -> Subquery -> body) — and deeper still
                            // for nested ORDER BY views — so hoist the
                            // subtree's flag onto this node to keep the line
                            // rendered (verified against sqlite3 3.51.0).
                            view_node.needs_temp_btree_for_order_by =
                                subtree_needs_order_by_temp_btree(&child);
                        }

                        view_node.add_child(child);
                        return Ok(view_node);
                    }
                }

                Self::explain_table_scan(
                    name,
                    alias.as_deref(),
                    where_clause,
                    order_by,
                    order_from_window,
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
                ..
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
                let left_child = Self::explain_from_clause(
                    left,
                    where_clause,
                    order_by,
                    order_from_window,
                    needed_columns,
                    database,
                    ctes,
                )?;
                join_node.add_child(left_child);

                // Add right child (no WHERE pushdown for right side in simple case)
                let empty_cols = HashSet::new();
                let right_child = Self::explain_from_clause(
                    right,
                    &None,
                    &None,
                    false,
                    &empty_cols,
                    database,
                    ctes,
                )?;
                join_node.add_child(right_child);

                Ok(join_node)
            }
            vibesql_ast::FromClause::Subquery { query, alias, .. } => {
                let mut subquery_node = PlanNode::new("Subquery");
                subquery_node.object = Some(format!("AS {}", alias));

                // Window-function subqueries cannot be flattened; SQLite
                // runs them as co-routines and EQP nests the inner plan
                // under a `CO-ROUTINE <alias>` entry (#5347). Plain derived
                // tables keep the existing flat rendering, matching
                // SQLite's query flattening.
                if Self::select_has_window_functions(query) {
                    subquery_node.coroutine = Some(alias.clone());
                }

                let child = Self::explain_select(query, database, ctes)?;
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
        order_from_window: bool,
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
            //
            // NOTE: If needed_columns is empty, it means SELECT * was used (wildcard),
            // which requires ALL table columns - this can never be a covering index
            // unless the index literally contains all columns.
            let is_covering = if needed_columns.is_empty() {
                // Wildcard case: need all table columns, cannot be a covering index
                false
            } else {
                let mut all_needed_columns = needed_columns.clone();
                if let Some(where_expr) = where_clause {
                    collect_column_refs(where_expr, &mut all_needed_columns);
                }
                // SQLite indexes implicitly carry the rowid, so an INTEGER
                // PRIMARY KEY column (rowid alias) never disqualifies a
                // covering index (windowpushd.test 1.4: index i1(grp_id)
                // covers `SELECT grp_id, id` when id is the rowid alias).
                if let Some(table) = database.get_table(table_name) {
                    if let Some(rowid_idx) = table.schema.rowid_alias_column {
                        if let Some(col) = table.schema.columns.get(rowid_idx) {
                            all_needed_columns.remove(&col.name.to_lowercase());
                        }
                    }
                }
                is_covering_index(&index_name, &all_needed_columns, database)
            };

            // Check if we have any predicates on the index (determines SCAN vs SEARCH)
            let has_index_predicates = if let Some(where_expr) = where_clause {
                let predicates = extract_index_predicates(where_expr, &index_name, database);
                !predicates.is_empty()
            } else {
                false
            };

            // Determine scan type:
            // - If we have predicates → SEARCH (or COVERING SEARCH)
            // - If no predicates but using index for ordering → SCAN USING INDEX
            let scan_type = if is_pk_lookup {
                ScanType::IntegerPrimaryKey
            } else if has_index_predicates {
                if is_covering {
                    ScanType::CoveringIndex
                } else {
                    ScanType::Search
                }
            } else {
                // No predicates - this is a SCAN using index for ordering
                ScanType::Scan
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
        } else if let Some(ordering_index) =
            order_by.as_deref().filter(|_| order_from_window).and_then(|items| {
                eqp_ordering_index(table_name, where_clause.as_ref(), items, database, true)
            })
        {
            // No filtering index, but the scan feeds a window sorting pass
            // whose key an index delivers: SQLite scans that index instead
            // of sorting (windowpushd.test 2.1.3.6: `SCAN t1 USING INDEX
            // i2` — i2 is chosen purely for PARTITION BY order).
            PlanNode::new("Index Scan")
                .with_object(table_name)
                .with_scan_type(ScanType::Scan)
                .with_index_name(&ordering_index)
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

/// Recursively check if an expression contains aggregate functions.
///
/// Used by the EQP view-flattening gate (#5355): a view body whose SELECT
/// list aggregates cannot be flattened. Subqueries are conservatively treated
/// as containing aggregates (blocking flattening keeps the safe opaque
/// rendering).
fn contains_aggregate_function(expr: &Expression) -> bool {
    match expr {
        Expression::AggregateFunction { .. } => true,
        Expression::Function { args, .. } => args.iter().any(contains_aggregate_function),
        Expression::BinaryOp { left, right, .. } => {
            contains_aggregate_function(left) || contains_aggregate_function(right)
        }
        Expression::UnaryOp { expr, .. } => contains_aggregate_function(expr),
        Expression::IsNull { expr, .. } => contains_aggregate_function(expr),
        Expression::Cast { expr, .. } => contains_aggregate_function(expr),
        Expression::Conjunction(exprs) | Expression::Disjunction(exprs) => {
            exprs.iter().any(contains_aggregate_function)
        }
        Expression::Case { operand, when_clauses, else_result, .. } => {
            operand.as_ref().is_some_and(|e| contains_aggregate_function(e))
                || when_clauses.iter().any(|clause| {
                    clause.conditions.iter().any(contains_aggregate_function)
                        || contains_aggregate_function(&clause.result)
                })
                || else_result.as_ref().is_some_and(|e| contains_aggregate_function(e))
        }
        Expression::InList { expr, values, .. } => {
            contains_aggregate_function(expr) || values.iter().any(contains_aggregate_function)
        }
        Expression::Between { expr, low, high, .. } => {
            contains_aggregate_function(expr)
                || contains_aggregate_function(low)
                || contains_aggregate_function(high)
        }
        // Conservative: subqueries may contain anything; block flattening.
        Expression::ScalarSubquery(_) | Expression::In { .. } | Expression::Exists { .. } => true,
        _ => false,
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
        _ => {} // Handle any other variants
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

    // Get index columns - only include column indexes, not expression indexes
    // Expression indexes cannot directly cover columns for covering index detection
    let index_columns: HashSet<String> = index
        .columns
        .iter()
        .filter_map(|c| c.column_name())
        .map(|name| name.to_lowercase())
        .collect();

    // Check if all needed columns are in the index
    needed_columns.iter().all(|col| index_columns.contains(col))
}

/// Extract index predicates from a WHERE clause for SQLite EQP format
///
/// Returns a list of (column_name, operator) tuples for predicates that can
/// use the index. The operator is one of "=", ">", "<", ">=", "<=".
/// Predicates are sorted by their position in the index column order.
/// For expression indexes, the expression name is used (e.g., "lower(name)").
fn extract_index_predicates(
    expr: &Expression,
    index_name: &str,
    database: &Database,
) -> Vec<(String, String)> {
    use vibesql_ast::pretty_print::ToSql;

    let mut predicates = Vec::new();

    // Get the index columns - for column indexes, use the column name
    // For expression indexes, use the expression as a string for display
    let index_columns: Vec<String> = database
        .get_index(index_name)
        .map(|idx| {
            idx.columns
                .iter()
                .map(|c| {
                    if let Some(name) = c.column_name() {
                        name.to_lowercase()
                    } else if let Some(expr) = c.get_expression() {
                        // For expression indexes, use the expression string
                        expr.to_sql().to_lowercase()
                    } else {
                        String::new()
                    }
                })
                .collect()
        })
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
    use vibesql_ast::pretty_print::ToSql;
    use vibesql_ast::BinaryOperator;

    /// Check if an expression matches any index column (column ref or expression index)
    fn expr_matches_index(expr: &Expression, index_columns: &[String]) -> Option<String> {
        match expr {
            Expression::ColumnRef(col_id) => {
                let col_name = col_id.column_canonical().to_lowercase();
                if index_columns.iter().any(|c| c == &col_name) {
                    Some(col_id.column_canonical().to_string())
                } else {
                    None
                }
            }
            // Check if the expression matches an expression index
            Expression::Function { .. } | Expression::BinaryOp { .. } => {
                let expr_str = expr.to_sql().to_lowercase();
                if index_columns.iter().any(|c| c == &expr_str) {
                    Some(expr.to_sql())
                } else {
                    None
                }
            }
            _ => None,
        }
    }

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
                // Check if left side matches an index column or expression index
                if let Some(expr_name) = expr_matches_index(left, index_columns) {
                    predicates.push((expr_name, op_str.to_string()));
                }
                // Check if right side matches (for reversed comparisons)
                else if let Some(expr_name) = expr_matches_index(right, index_columns) {
                    // Reverse the operator for column on right side
                    let reversed_op = match op {
                        BinaryOperator::Equal => "=",
                        BinaryOperator::LessThan => ">",
                        BinaryOperator::LessThanOrEqual => ">=",
                        BinaryOperator::GreaterThan => "<",
                        BinaryOperator::GreaterThanOrEqual => "<=",
                        _ => return,
                    };
                    predicates.push((expr_name, reversed_op.to_string()));
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
        // IS (NULL-safe equals): negated=true means "IS NOT DISTINCT FROM" = "IS"
        // Displayed as = in EQP output
        Expression::IsDistinctFrom { left, right, negated: true } => {
            // Check if left side is a column reference that matches index columns
            if let Expression::ColumnRef(col_id) = left.as_ref() {
                let col_name = col_id.column_canonical().to_lowercase();
                if index_columns.iter().any(|c| c == &col_name) {
                    predicates.push((col_id.column_canonical().to_string(), "=".to_string()));
                }
            }
            // Check if right side is a column reference
            else if let Expression::ColumnRef(col_id) = right.as_ref() {
                let col_name = col_id.column_canonical().to_lowercase();
                if index_columns.iter().any(|c| c == &col_name) {
                    predicates.push((col_id.column_canonical().to_string(), "=".to_string()));
                }
            }
        }
        _ => {}
    }
}
