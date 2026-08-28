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

use std::{collections::HashSet, fmt::Write};

use vibesql_ast::{
    pretty_print::ToSql, DeleteStmt, ExplainFormat, ExplainStmt, Expression, FromClause,
    SelectItem, SelectStmt, Statement, UpdateStmt, WhereClause,
};
use vibesql_storage::Database;

use crate::{
    errors::ExecutorError,
    optimizer::index_planner::IndexPlanner,
    select::scan::index_scan::{
        collect_equality_pinned_columns_with_collation, cost_based_index_selection,
        eqp_ordering_index, needs_temp_btree_for_order_by_eqp, select_index_scan_method,
        EqualityPinnedColumn, IndexScanChoice,
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
    /// True when `index_name` covers every column the query reads from this
    /// table (SELECT list + WHERE, with the rowid alias carried implicitly).
    /// Ordering scans then render as SQLite's
    /// `SCAN <table> USING COVERING INDEX <index>` (#5371).
    pub index_covering: bool,
    /// Index predicates for SQLite EQP format (e.g., "w=?", "x>? AND x<?")
    pub index_predicates: Vec<IndexPredicate>,
    /// Whether this query requires a temp B-tree for ORDER BY
    pub needs_temp_btree_for_order_by: bool,
    /// Whether this query requires a temp structure for GROUP BY — rendered
    /// as SQLite's `USE TEMP B-TREE FOR GROUP BY`. VibeSQL's runtime always
    /// groups via a hash table and then sorts the groups by key
    /// (select/grouping/hash.rs), so the line truthfully describes the temp
    /// grouping structure; it is suppressed when an index delivers group
    /// order, mirroring SQLite's EQP (same permissive EQP-level convention
    /// as the ORDER BY stabilization-sort suppression in
    /// `needs_temp_btree_for_order_by_eqp`).
    pub needs_temp_btree_for_group_by: bool,
    /// Whether this query requires a temp structure for DISTINCT — rendered
    /// as SQLite's `USE TEMP B-TREE FOR DISTINCT`. VibeSQL's runtime dedups
    /// via a hash set preserving input order (select/helpers.rs
    /// `apply_distinct`); the line truthfully describes the temp dedup
    /// structure and is suppressed when an index delivers the SELECT-list
    /// order, mirroring SQLite's EQP.
    pub needs_temp_btree_for_distinct: bool,
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
    /// When non-empty, this node is a single-table MULTI-INDEX OR access path
    /// (epic #5668): one per-branch index `SEARCH` PlanNode, in original-branch
    /// ordinal order. EQP renders it as SQLite's `MULTI-INDEX OR` subtree:
    /// ```text
    /// MULTI-INDEX OR
    /// |--INDEX <ordinal>
    /// |  `--SEARCH <table> USING INDEX <idx> (<pred>)
    /// `--INDEX <ordinal>
    ///    `--SEARCH ...
    /// ```
    /// Each tuple is `(ordinal, branch_search_node)` where `ordinal` is the
    /// 1-based term position in the original OR expression (preserved from the
    /// analyzer — SQLite labels branches by original position, not a
    /// renumbering over chosen branches).
    pub multi_index_or_branches: Vec<(usize, PlanNode)>,
    /// When true, this scan is the inner side of a LEFT (outer) join, so its
    /// SEARCH line carries SQLite's trailing ` LEFT-JOIN` marker
    /// (where9-3.2). Applies to ordinary SEARCH/COVERING lines and to each
    /// inner `SEARCH` line of a correlated-join MULTI-INDEX OR subtree.
    pub left_join: bool,
    /// When set, this node is an `EXISTS`/`IN`/scalar-subquery expression
    /// SQLite cannot flatten into the outer FROM-clause join/semi-join plan
    /// (a correlated WHERE-clause `EXISTS`/`IN` whose own subquery has a
    /// multi-table FROM clause or aggregates without `GROUP BY`, or any
    /// SELECT-list `EXISTS`/`IN`/scalar-subquery expression, which is never
    /// rewritten into a join at all). Rendered as its own labelled entry
    /// (e.g. `CORRELATED SCALAR SUBQUERY 1`) with the subquery's own plan
    /// nested underneath, matching sqlite3's EQP shape (existsexpr.test
    /// 3.7/3.9/4.4, #6647).
    pub subquery_label: Option<String>,
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
            index_covering: false,
            index_predicates: Vec::new(),
            needs_temp_btree_for_order_by: false,
            needs_temp_btree_for_group_by: false,
            needs_temp_btree_for_distinct: false,
            window_sort_count: 0,
            set_operation_type: None,
            is_compound_query: false,
            coroutine: None,
            multi_index_or_branches: Vec::new(),
            left_join: false,
            subquery_label: None,
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
    // Un-flattenable EXISTS/IN/scalar-subquery expression (#6647): render as
    // its own labelled entry (`CORRELATED SCALAR SUBQUERY N`) with the
    // subquery's own plan nested underneath, mirroring the CO-ROUTINE/
    // COMPOUND QUERY nesting pattern below.
    if let Some(ref label) = node.subquery_label {
        let mut children = Vec::new();
        for child in &node.children {
            children.extend(collect_eqp_entries(child));
        }
        entries.push(EqpEntry { text: label.clone(), children });
        return;
    }

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

    // Single-table MULTI-INDEX OR (epic #5668): render SQLite's subtree
    //   MULTI-INDEX OR
    //   |--INDEX <ordinal>
    //   |  `--SEARCH <table> USING [COVERING ]INDEX <idx> (<pred>)
    //   `--INDEX <ordinal>
    //      `--SEARCH ...
    // Each branch is an `INDEX <ordinal>` entry whose only child is the branch's
    // SEARCH line, reusing `format_sqlite_eqp_node` so covering-index detection
    // and predicate formatting stay byte-identical to ordinary single-index
    // SEARCH lines. Ordinals are the original OR-term positions (preserved by the
    // analyzer), not a renumbering over the chosen branches.
    if !node.multi_index_or_branches.is_empty() {
        let branch_entries: Vec<EqpEntry> = node
            .multi_index_or_branches
            .iter()
            .map(|(ordinal, search_node)| EqpEntry {
                text: format!("INDEX {}", ordinal),
                children: vec![EqpEntry::leaf(format_sqlite_eqp_node(search_node))],
            })
            .collect();
        entries.push(EqpEntry { text: "MULTI-INDEX OR".to_string(), children: branch_entries });
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
/// set operation (`UNION USING TEMP B-TREE`, `UNION ALL`, ...), each
/// containing that branch's full EQP entries — including the branch's own
/// `USE TEMP B-TREE FOR GROUP BY` / `FOR DISTINCT` lines, which sqlite3
/// renders inside the branch block (verified live).
fn compound_eqp_entry(node: &PlanNode) -> EqpEntry {
    let mut children = Vec::new();
    for (i, branch) in node.children.iter().enumerate() {
        let label = if i == 0 {
            "LEFT-MOST SUBQUERY".to_string()
        } else {
            branch.set_operation_type.clone().unwrap_or_else(|| "UNION ALL".to_string())
        };
        children.push(EqpEntry { text: label, children: collect_eqp_entries(branch) });
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
///
/// Line order matches sqlite3 3.51.0: scans, then `... FOR GROUP BY`, then
/// window sorting passes, then `... FOR DISTINCT`, then `... FOR ORDER BY`
/// (GROUP BY-before-DISTINCT and DISTINCT/GROUP BY-before-ORDER BY verified
/// live; windows restructure SQLite's plan entirely, so their relative slot
/// follows the grouping-before-dedup pipeline order).
fn collect_eqp_entries(node: &PlanNode) -> Vec<EqpEntry> {
    let mut entries = Vec::new();

    // Collect scan entries first (co-routine blocks nest their inner plan)
    append_scan_entries(node, &mut entries);

    // Add TEMP B-TREE entry if needed for GROUP BY
    if node.needs_temp_btree_for_group_by {
        entries.push(EqpEntry::leaf("USE TEMP B-TREE FOR GROUP BY".to_string()));
    }

    // Add one TEMP B-TREE entry per distinct window sort key not satisfied
    // by an index. Window sorting passes run before the statement-level
    // ORDER BY, and SQLite never dedups them against it — they are
    // separate passes.
    for _ in 0..node.window_sort_count {
        entries.push(EqpEntry::leaf("USE TEMP B-TREE FOR ORDER BY".to_string()));
    }

    // Add TEMP B-TREE entry if needed for DISTINCT
    if node.needs_temp_btree_for_distinct {
        entries.push(EqpEntry::leaf("USE TEMP B-TREE FOR DISTINCT".to_string()));
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
            // Check if we have an index for ordering (SCAN table USING INDEX idx).
            // When the index also covers every column read, SQLite renders
            // `SCAN <table> USING COVERING INDEX <index>` (verified live
            // against sqlite3 3.51.0, #5371).
            if let Some(ref index_name) = node.index_name {
                if node.index_covering {
                    format!("SCAN {} USING COVERING INDEX {}", table_name, index_name)
                } else {
                    format!("SCAN {} USING INDEX {}", table_name, index_name)
                }
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

            let suffix = if node.left_join { " LEFT-JOIN" } else { "" };
            if node.index_predicates.is_empty() {
                format!("SEARCH {} USING {}INDEX {}{}", table_name, covering, index_name, suffix)
            } else {
                let predicates: Vec<String> = node
                    .index_predicates
                    .iter()
                    .map(|p| format!("{}{}?", p.column, p.predicate_type))
                    .collect();
                format!(
                    "SEARCH {} USING {}INDEX {} ({}){}",
                    table_name,
                    covering,
                    index_name,
                    predicates.join(" AND "),
                    suffix
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
            Statement::Update(update_stmt) => Self::explain_update(update_stmt, database)?,
            Statement::Delete(delete_stmt) => Self::explain_delete(delete_stmt, database)?,
            _ => {
                return Err(ExecutorError::Other(
                    "EXPLAIN only supports SELECT, INSERT, UPDATE, DELETE statements".to_string(),
                ));
            }
        };

        Ok(ExplainResult { plan, format: stmt.format.clone() })
    }

    /// Generate an execution plan for a DELETE statement.
    ///
    /// Accessing the target table of a `DELETE FROM t WHERE ...` plans
    /// exactly like `SELECT rowid FROM t WHERE ...` (verified against
    /// sqlite3 3.51.0) — both walk the same table via the same scan/search
    /// access-path selection. This reuses [`Self::explain_from_clause`] on a
    /// synthetic single-table FROM clause built from the statement's target
    /// table and index hint, so `EXPLAIN QUERY PLAN DELETE FROM t WHERE ...`
    /// renders the real `SCAN`/`SEARCH` line instead of an empty plan
    /// (previously this produced no scan entries at all — e_fkey.test
    /// e_fkey-25.2).
    ///
    /// Not modeled: when foreign key enforcement is active, SQLite also
    /// plans one child-table orphan-check subquery per foreign key that
    /// references this table's key columns
    /// (`SELECT rowid FROM <child> WHERE <child-key> = ?`, e_fkey.test
    /// section 4 EVIDENCE-OF R-00279-52283/R-23302-30956). That automatic
    /// FK-check sub-plan is a separate, larger feature — see e_fkey-25.3/
    /// 26.x, tracked in issue #6170.
    fn explain_delete(stmt: &DeleteStmt, database: &Database) -> Result<PlanNode, ExecutorError> {
        let mut root = PlanNode::new("Delete");
        let where_expr = Self::where_clause_condition(stmt.where_clause.as_ref());
        let from = FromClause::Table {
            name: stmt.table_name.clone(),
            alias: stmt.alias.clone(),
            column_aliases: None,
            quoted: stmt.quoted,
            index_hint: stmt.index_hint.clone(),
        };
        let scan_node = Self::explain_from_clause(
            &from,
            &where_expr,
            &None,
            false,
            &HashSet::new(),
            database,
            &HashSet::new(),
        )?;
        root.add_child(scan_node);
        if where_expr.is_some() {
            root.details.push("Filter: <where clause>".to_string());
        }
        Ok(root)
    }

    /// Generate an execution plan for an UPDATE statement.
    ///
    /// Same rationale as [`Self::explain_delete`]: the target table's
    /// access path for `UPDATE t SET ... WHERE ...` is planned exactly like
    /// scanning/searching it in a SELECT (e_fkey.test e_fkey-27.x's
    /// `EXPLAIN QUERY PLAN UPDATE artist SET ...` expects the same `SCAN`/
    /// `SEARCH` line a `SELECT` over the same WHERE clause would produce).
    ///
    /// The SQLite 3.33+ `UPDATE ... FROM <other-tables>` extension is not
    /// modeled here — only the target table's own scan is rendered; a
    /// present `from_clause` does not currently contribute additional scan
    /// entries. Not modeled either: SQLite's automatic FK child orphan-check
    /// sub-plan for parent-key updates (e_fkey-27.3/27.4, see
    /// [`Self::explain_delete`]'s doc comment and issue #6170).
    fn explain_update(stmt: &UpdateStmt, database: &Database) -> Result<PlanNode, ExecutorError> {
        let mut root = PlanNode::new("Update");
        let where_expr = Self::where_clause_condition(stmt.where_clause.as_ref());
        let from = FromClause::Table {
            name: stmt.table_name.clone(),
            alias: stmt.alias.clone(),
            column_aliases: None,
            quoted: stmt.quoted,
            index_hint: stmt.index_hint.clone(),
        };
        let scan_node = Self::explain_from_clause(
            &from,
            &where_expr,
            &None,
            false,
            &HashSet::new(),
            database,
            &HashSet::new(),
        )?;
        root.add_child(scan_node);
        if where_expr.is_some() {
            root.details.push("Filter: <where clause>".to_string());
        }
        Ok(root)
    }

    /// Extract the plain boolean condition from a positioned-update-aware
    /// `WhereClause`, dropping `WHERE CURRENT OF <cursor>` (which names a
    /// cursor rather than a filter expression and has no scan/search
    /// counterpart to render).
    fn where_clause_condition(where_clause: Option<&WhereClause>) -> Option<Expression> {
        match where_clause {
            Some(WhereClause::Condition(expr)) => Some(expr.clone()),
            _ => None,
        }
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

        // Temp-structure suppression keys (#5367), computed BEFORE the FROM
        // analysis because the base scan's effective ordering requirement
        // depends on which suppression (if any) fires (#5371): when an index
        // delivers GROUP BY / DISTINCT order, sqlite3 shows the scan riding
        // that index (`SCAN t USING [COVERING ]INDEX i`, verified live).
        //
        // The simple GROUP BY key (ordinals and output aliases resolved like
        // SQLite) is also reused below to suppress the ORDER BY line when the
        // statement-level ORDER BY matches the grouping output order.
        let group_key: Option<Vec<&Expression>> =
            stmt.group_by.as_ref().map(|g| g.as_simple()).and_then(|simple| {
                simple.map(|exprs| {
                    exprs.iter().map(|e| Self::resolve_output_expr(e, &stmt.select_list)).collect()
                })
            });
        // The index-delivered group order (as written or permuted into an
        // index's column order); `Some` exactly when the GROUP BY temp line
        // is suppressed.
        let group_index_order: Option<Vec<vibesql_ast::OrderByItem>> = if stmt.group_by.is_some() {
            group_key.as_ref().and_then(|key| {
                Self::group_key_index_order(
                    stmt.from.as_ref(),
                    stmt.where_clause.as_ref(),
                    key,
                    database,
                )
            })
        } else {
            None
        };

        // The DISTINCT key is the SELECT list. SQLite suppresses the line
        // when an index delivers the SELECT-list order — but never when a
        // GROUP BY intervenes (`SELECT DISTINCT a FROM t GROUP BY a` with an
        // index on `a` still shows the DISTINCT line; verified live).
        let distinct_key: Option<Vec<&Expression>> =
            if stmt.distinct { Self::distinct_key_exprs(&stmt.select_list) } else { None };
        // The index-delivered SELECT-list order for DISTINCT; `Some` exactly
        // when the DISTINCT temp line is suppressed.
        let distinct_index_order: Option<Vec<vibesql_ast::OrderByItem>> =
            if stmt.distinct && stmt.group_by.is_none() {
                distinct_key.as_ref().and_then(|key| {
                    // Columns the WHERE clause constrains to a single value are
                    // constant within the scan output, so SQLite drops them from
                    // the distinctness key before deciding whether an index can
                    // deliver it (orderby5 1.1–1.6). The collation of the WHERE
                    // comparison must match the collation the DISTINCT applies to
                    // the column, or the pin does not make it constant for
                    // distinctness (orderby5 1.2.2 / 1.2.3).
                    let pinned =
                        collect_equality_pinned_columns_with_collation(stmt.where_clause.as_ref());
                    let mut removed: Vec<&Expression> = Vec::new();
                    let mut unpinned_key: Vec<&Expression> = Vec::new();
                    for e in key.iter().copied() {
                        if Self::distinct_expr_is_where_pinned(e, &pinned) {
                            removed.push(e);
                        } else {
                            unpinned_key.push(e);
                        }
                    }
                    if unpinned_key.is_empty() {
                        // Every DISTINCT column is constant — the result has at
                        // most one distinct row, so no temp B-tree is needed.
                        return Some(Vec::new());
                    }
                    // The equality predicate that pinned a removed column is
                    // "consumed" by the distinctness reduction. Drop it from the
                    // WHERE before the index-delivery check so a column pinned to
                    // a constant (and NOT itself indexed, e.g. `a=0` over
                    // `t1bc(b,c)`) does not look like a competing access path that
                    // would otherwise block riding the ordering index.
                    let removed_columns: Vec<String> =
                        removed.iter().filter_map(|e| Self::distinct_key_column_name(e)).collect();
                    let reduced_where = Self::where_without_pinned_columns(
                        stmt.where_clause.as_ref(),
                        &removed_columns,
                    );
                    // DISTINCT is order-insensitive, so the index may deliver any
                    // permutation of the reduced key (orderby5 1.2.1 / 1.5 / 1.6:
                    // `DISTINCT a, c, b WHERE a=0` reduces to `(c, b)` which index
                    // t1bc(b, c) covers after reordering to `(b, c)`).
                    Self::key_index_order(
                        stmt.from.as_ref(),
                        reduced_where.as_ref(),
                        &unpinned_key,
                        database,
                    )
                })
            } else {
                None
            };

        // Analyze FROM clause. The base scan's effective ordering
        // requirement follows SQLite's pipeline priority:
        // - When the SELECT list contains window functions, the scan feeds the INNERMOST window
        //   sorting pass (SQLite's co-routine rewrite, see count_window_sorts) — SQLite picks an
        //   index that delivers PARTITION BY/ORDER BY order even without any predicate
        //   (windowpushd.test 2.1.3.6).
        // - Otherwise the grouping/dedup pass consumes the scan's order: when its suppression
        //   fired, the scan rides the delivering index (#5371, sqlite3 3.51.0 verified live).
        // - Otherwise the statement-level ORDER BY applies; the scan may ride an ordering index
        //   exactly when the ORDER BY temp-line suppression fires, so non-suppressed shapes keep
        //   their pre-existing rendering.
        //
        // The scan rendering and the suppression stay COUPLED on purpose
        // (#5373, investigated): sqlite3 also rides an index satisfying only
        // a partial prefix of the key while keeping the temp line (partial
        // GROUP BY/DISTINCT keys, mixed ASC/DESC — it then renames the sort
        // line `USE TEMP B-TREE FOR LAST TERM OF ORDER BY`), but VibeSQL's
        // runtime never does: `cost_based_index_selection` accepts an index
        // for ordering only on a full direction-uniform match, and the
        // aggregation path passes no ordering hint to the scan at all. In
        // every partial case the runtime seq-scans, so an uncoupled
        // `SCAN t USING INDEX i` would misstate the access path — documented
        // divergence (explain_temp_btree_annotation_tests.rs, #5373 section).
        let window_scan_key = Self::distinct_window_keys(stmt).pop();
        let (scan_order_by, prefer_ordering_scan): (Option<Vec<vibesql_ast::OrderByItem>>, bool) =
            if window_scan_key.is_some() {
                (window_scan_key, true)
            } else if group_index_order.is_some() {
                (group_index_order.clone(), true)
            } else if distinct_index_order.is_some() {
                (distinct_index_order.clone(), true)
            } else if stmt.group_by.is_none() && !stmt.distinct {
                let order_by_suppressed = stmt.order_by.as_ref().is_some_and(|ob| {
                    !Self::needs_temp_btree_for_order_by(
                        stmt.from.as_ref(),
                        stmt.where_clause.as_ref(),
                        ob,
                        database,
                    )
                });
                (stmt.order_by.clone(), order_by_suppressed)
            } else {
                (stmt.order_by.clone(), false)
            };
        if let Some(ref from_clause) = stmt.from {
            let scan_node = Self::explain_from_clause(
                from_clause,
                &stmt.where_clause,
                &scan_order_by,
                prefer_ordering_scan,
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

        // Temp-structure annotations for GROUP BY and DISTINCT (#5367).
        //
        // SQLite emits `USE TEMP B-TREE FOR GROUP BY` / `FOR DISTINCT` when
        // grouping/dedup is not satisfied by the scan's delivery order, and
        // suppresses the line when an index delivers it (verified against
        // sqlite3 3.51.0). VibeSQL's runtime always hash-groups then sorts
        // groups by key, and always hash-dedups DISTINCT — the emitted lines
        // truthfully describe those temp structures; the index suppression
        // mirrors the established permissive EQP-level convention (see
        // `needs_temp_btree_for_order_by_eqp`).
        if stmt.group_by.is_some() {
            // ROLLUP/CUBE/GROUPING SETS (`group_key` is None) always build
            // temp structures; a simple key suppresses exactly when an index
            // delivers group order (`group_index_order`, computed above).
            root.needs_temp_btree_for_group_by = group_index_order.is_none();
        }
        if stmt.distinct {
            // Wildcard SELECT lists and grouped queries never suppress;
            // `distinct_index_order` (computed above) is `Some` exactly when
            // an index delivers the SELECT-list order.
            root.needs_temp_btree_for_distinct = distinct_index_order.is_none();
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
        //
        // With GROUP BY, the grouping pass replaces the scan as the order
        // source: SQLite suppresses the ORDER BY line exactly when the ORDER
        // BY terms equal the GROUP BY terms — same expressions, same
        // sequence, same length, directions ignored (the group structure is
        // traversed in either direction); a bare prefix does NOT suppress
        // (all verified against sqlite3 3.51.0). With DISTINCT, the dedup
        // structure delivers SELECT-list order: an exact all-ASC match
        // suppresses; otherwise the line renders unless the dedup itself
        // rode an index (in which case the index check applies as usual).
        if let Some(ref order_by) = stmt.order_by {
            let satisfied_by_window_sort =
                Self::first_window_combined_key(stmt).is_some_and(|key| {
                    !key.is_empty()
                        && order_by.len() <= key.len()
                        && key[..order_by.len()] == order_by[..]
                });
            root.needs_temp_btree_for_order_by = if satisfied_by_window_sort {
                false
            } else if stmt.group_by.is_some() {
                match &group_key {
                    Some(key) => !Self::order_by_matches_exprs(
                        order_by,
                        key,
                        &stmt.select_list,
                        false, // directions ignored
                    ),
                    // ROLLUP/CUBE/GROUPING SETS output order is unspecified.
                    None => true,
                }
            } else if stmt.distinct {
                let matches_distinct = distinct_key.as_ref().is_some_and(|key| {
                    Self::order_by_matches_exprs(
                        order_by,
                        key,
                        &stmt.select_list,
                        true, // exact ASC match required (verified live)
                    )
                });
                if matches_distinct {
                    false
                } else if root.needs_temp_btree_for_distinct {
                    // Hash/b-tree dedup does not deliver the requested order.
                    true
                } else {
                    // Dedup rode the index; the scan's delivery order may
                    // still satisfy the ORDER BY (e.g. reverse traversal).
                    Self::needs_temp_btree_for_order_by(
                        stmt.from.as_ref(),
                        stmt.where_clause.as_ref(),
                        order_by,
                        database,
                    )
                }
            } else {
                Self::needs_temp_btree_for_order_by(
                    stmt.from.as_ref(),
                    stmt.where_clause.as_ref(),
                    order_by,
                    database,
                )
            };
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

        // Un-flattenable EXISTS/IN/scalar-subquery expressions (#6647):
        // SQLite's exists-to-join optimizer can rewrite a simple,
        // single-table correlated WHERE-clause `EXISTS`/`IN` into a
        // semi-join with no separate plan entry (the common case, already
        // rendered above via the bare outer scan) — but a subquery whose own
        // FROM clause has more than one table, or whose own SELECT list
        // aggregates without a `GROUP BY` (always exactly one row, so it
        // cannot become a per-outer-row join predicate), keeps its own
        // `CORRELATED SCALAR SUBQUERY N` entry (existsexpr.test 3.7/3.9). A
        // SELECT-list `EXISTS`/`IN`/scalar-subquery expression is never
        // rewritten into a join at all, so it always keeps its own entry
        // (existsexpr.test 4.4).
        let mut where_subqueries = Vec::new();
        if let Some(ref where_expr) = stmt.where_clause {
            Self::collect_unflattenable_where_subqueries(where_expr, &mut where_subqueries);
        }
        for (subquery_index, subquery) in where_subqueries
            .into_iter()
            .chain(Self::collect_select_list_subqueries(&stmt.select_list))
            .enumerate()
        {
            let mut subquery_node = PlanNode::new("Subquery");
            subquery_node.subquery_label =
                Some(format!("CORRELATED SCALAR SUBQUERY {}", subquery_index + 1));
            let child = Self::explain_select(subquery, database, &ctes)?;
            subquery_node.add_child(child);
            root.add_child(subquery_node);
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
            hints: Vec::new(),
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
            // Dedup branches are labeled `<OP> USING TEMP B-TREE` like
            // sqlite3 3.51.0 (verified live for UNION/INTERSECT/EXCEPT).
            // Truthful: the runtime dedups via temp hash structures
            // (select/set_operations.rs). `ALL` variants stay bare; the
            // non-standard INTERSECT ALL / EXCEPT ALL have no SQLite
            // reference output and keep their bare labels.
            let op_label = match (&set_op.op, set_op.all) {
                (vibesql_ast::SetOperator::Union, true) => "UNION ALL",
                (vibesql_ast::SetOperator::Union, false) => "UNION USING TEMP B-TREE",
                (vibesql_ast::SetOperator::Intersect, true) => "INTERSECT ALL",
                (vibesql_ast::SetOperator::Intersect, false) => "INTERSECT USING TEMP B-TREE",
                (vibesql_ast::SetOperator::Except, true) => "EXCEPT ALL",
                (vibesql_ast::SetOperator::Except, false) => "EXCEPT USING TEMP B-TREE",
            };

            // Create plan for right side of set operation
            let right_stmt = SelectStmt {
                hints: Vec::new(),
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

        // A compound's statement-level ORDER BY renders as a trailing
        // `USE TEMP B-TREE FOR ORDER BY` line after the COMPOUND QUERY
        // block. Truthful (#5371): the runtime materializes the combined
        // result and sorts it in one pass (`sort_set_operation_results`,
        // select/executor/execute.rs) for every operator, ALL or not.
        //
        // Documented divergence: sqlite3 3.51.0 instead renders a
        // `MERGE (UNION)` / `MERGE (UNION ALL)` / ... block with per-branch
        // `USE TEMP B-TREE FOR ORDER BY` lines (verified live) because its
        // runtime sorts each branch and merges them. VibeSQL does not
        // merge pre-sorted branches, so rendering MERGE would fabricate a
        // plan shape that never executes (per the #5355/#5360/#5366
        // truthfulness precedent).
        //
        // Narrow carve-out: when EVERY branch is a constant-row query (no FROM,
        // SELECT list of literals only — e.g. `SELECT 5 UNION ALL SELECT 3`),
        // there is no table to sort and SQLite emits no temp B-tree for the
        // ORDER BY (orderby1 5.1). The runtime likewise sorts a tiny constant
        // result without a table-backed temp structure, so suppressing the line
        // is truthful for this shape.
        root.needs_temp_btree_for_order_by =
            stmt.order_by.is_some() && !Self::all_compound_branches_are_constant(stmt);

        Ok(root)
    }

    /// True when every branch of a compound (set-operation) query is a
    /// constant-row query: no FROM clause and a SELECT list consisting solely of
    /// literal expressions (or a VALUES body of literals). SQLite computes the
    /// ORDER BY of such an all-constant compound without a temp B-tree
    /// (orderby1 5.1).
    fn all_compound_branches_are_constant(stmt: &SelectStmt) -> bool {
        fn expr_is_literal(expr: &Expression) -> bool {
            matches!(expr, Expression::Literal(_))
        }
        fn branch_is_constant(stmt: &SelectStmt) -> bool {
            if stmt.from.is_some() {
                return false;
            }
            if let Some(rows) = &stmt.values {
                return rows.iter().all(|row| row.iter().all(expr_is_literal));
            }
            !stmt.select_list.is_empty()
                && stmt.select_list.iter().all(|item| match item {
                    SelectItem::Expression { expr, .. } => expr_is_literal(expr),
                    SelectItem::Wildcard { .. } | SelectItem::QualifiedWildcard { .. } => false,
                })
        }

        if !branch_is_constant(stmt) {
            return false;
        }
        let mut current = stmt.set_operation.as_ref();
        while let Some(set_op) = current {
            if !branch_is_constant(&set_op.right) {
                return false;
            }
            current = set_op.right.set_operation.as_ref();
        }
        true
    }

    /// Count the distinct window-function sorting passes required by the
    /// SELECT list that are not satisfied by an index.
    ///
    /// SQLite semantics (window1.test section 23, `do_ordercount_test`):
    /// - The sort key for a window is its PARTITION BY expressions (treated as ASC with default
    ///   null ordering) followed by its ORDER BY items. `OVER (PARTITION BY a ORDER BY b)` and
    ///   `OVER (ORDER BY a, b)` share the key `(a, b)`.
    /// - Keys are deduplicated by exact structural equality (including direction and COLLATE);
    ///   frame clauses are ignored entirely.
    /// - Windows with neither PARTITION BY nor ORDER BY (`OVER ()`) need no sorting pass.
    /// - Only the INNERMOST sorting pass scans the base table and can be satisfied by an index
    ///   (e.g. key `(a, b)` with index `t5ab(a, b)`). SQLite's nested co-routine rewrite emits
    ///   passes in reverse SELECT-list order, so the innermost pass corresponds to the LAST
    ///   distinct key (by first occurrence).
    /// - When the innermost pass IS satisfied by an index, sortedness propagates outward: each
    ///   outer pass whose key is a structural prefix of the order delivered by the pass below it
    ///   needs no sort either, because the rows flow through in an order that already satisfies it
    ///   (window9.test 5.1.1: index `i1(a,b,c,d,e)` satisfies keys `(a,b,c,d)`, `(a,b,c)`, `(a,b)`
    ///   and `(a)` — zero sorts). The chain breaks at the first outer key that is not such a
    ///   prefix; that key and all keys outside it need temp B-trees (the temp B-tree sort is not
    ///   guaranteed to preserve residual ordering, so no further propagation is attempted —
    ///   matching the conservative pre-existing behavior validated by window1.test section 23).
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

    /// Resolve an ORDER BY / GROUP BY term against the SELECT list the way
    /// SQLite does for EQP purposes: an integer literal is a 1-based output
    /// ordinal, and a bare (unqualified) column reference matching an output
    /// alias resolves to that output expression. Anything else (including
    /// out-of-range ordinals) is returned unchanged. Verified live:
    /// `GROUP BY b ORDER BY 1` and `SELECT b AS z ... GROUP BY b ORDER BY z`
    /// both suppress the ORDER BY temp B-tree line.
    fn resolve_output_expr<'a>(
        expr: &'a Expression,
        select_list: &'a [SelectItem],
    ) -> &'a Expression {
        use vibesql_types::SqlValue;

        let ordinal = match expr {
            Expression::Literal(SqlValue::Integer(n))
            | Expression::Literal(SqlValue::Bigint(n)) => Some(*n),
            Expression::Literal(SqlValue::Smallint(n)) => Some(i64::from(*n)),
            _ => None,
        };
        if let Some(n) = ordinal {
            if n >= 1 && (n as usize) <= select_list.len() {
                if let SelectItem::Expression { expr: target, .. } = &select_list[n as usize - 1] {
                    return target;
                }
            }
            return expr;
        }

        if let Expression::ColumnRef(col_id) = expr {
            if col_id.schema_canonical().is_none() && col_id.table_canonical().is_none() {
                for item in select_list {
                    if let SelectItem::Expression { expr: target, alias: Some(alias), .. } = item {
                        if alias.eq_ignore_ascii_case(col_id.column_canonical()) {
                            return target;
                        }
                    }
                }
            }
        }

        expr
    }

    /// The DISTINCT key: the SELECT-list expressions in order, or `None`
    /// when the list contains wildcards (which never suppress the DISTINCT
    /// temp-structure line).
    fn distinct_key_exprs(select_list: &[SelectItem]) -> Option<Vec<&Expression>> {
        select_list
            .iter()
            .map(|item| match item {
                SelectItem::Expression { expr, .. } => Some(expr),
                SelectItem::Wildcard { .. } | SelectItem::QualifiedWildcard { .. } => None,
            })
            .collect()
    }

    /// True when a DISTINCT-key expression is a (possibly `COLLATE`-wrapped)
    /// reference to a column the WHERE clause constrains to a single value under
    /// the *same* collation. Such a column is constant across the scan output and
    /// contributes nothing to distinctness, so SQLite removes it from the
    /// distinctness key (orderby5 1.1–1.6).
    ///
    /// The collation must match: `SELECT DISTINCT a ... WHERE a='x' COLLATE
    /// nocase` does not make BINARY-collated `a` constant (orderby5 1.2.2), and
    /// `SELECT DISTINCT a COLLATE nocase ... WHERE a='x'` is not made constant by
    /// a BINARY pin (orderby5 1.2.3). A bare column with no explicit COLLATE on
    /// either the key or the pin matches (both default to BINARY).
    fn distinct_expr_is_where_pinned(expr: &Expression, pinned: &[EqualityPinnedColumn]) -> bool {
        let (column, key_collation) = match expr {
            Expression::ColumnRef(col_id) => (col_id.column_canonical().to_uppercase(), None),
            Expression::Collate { expr: inner, collation } => {
                if let Expression::ColumnRef(col_id) = &**inner {
                    (col_id.column_canonical().to_uppercase(), Some(collation.to_lowercase()))
                } else {
                    return false;
                }
            }
            _ => return false,
        };
        pinned
            .iter()
            .any(|p| p.column.eq_ignore_ascii_case(&column) && p.collation == key_collation)
    }

    /// The uppercased column name of a DISTINCT-key expression that is a bare
    /// column or a single `COLLATE`-wrapped column; `None` otherwise.
    fn distinct_key_column_name(expr: &Expression) -> Option<String> {
        match expr {
            Expression::ColumnRef(col_id) => Some(col_id.column_canonical().to_uppercase()),
            Expression::Collate { expr: inner, .. } => {
                if let Expression::ColumnRef(col_id) = &**inner {
                    Some(col_id.column_canonical().to_uppercase())
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Rebuild a WHERE clause with the top-level `col = literal` equality
    /// conjuncts on any of `removed_columns` dropped (the distinctness reduction
    /// already accounted for them as constants). Returns `None` when nothing
    /// remains. Only AND/Conjunction nesting is descended into; any other shape
    /// is preserved verbatim.
    fn where_without_pinned_columns(
        where_clause: Option<&Expression>,
        removed_columns: &[String],
    ) -> Option<Expression> {
        fn is_removed_equality(expr: &Expression, removed: &[String]) -> bool {
            let col = match expr {
                Expression::BinaryOp { left, op: vibesql_ast::BinaryOperator::Equal, right } => {
                    let from_side = |side: &Expression| -> Option<String> {
                        match side {
                            Expression::ColumnRef(c) => Some(c.column_canonical().to_uppercase()),
                            Expression::Collate { expr: inner, .. } => {
                                if let Expression::ColumnRef(c) = &**inner {
                                    Some(c.column_canonical().to_uppercase())
                                } else {
                                    None
                                }
                            }
                            _ => None,
                        }
                    };
                    from_side(left).or_else(|| from_side(right))
                }
                _ => None,
            };
            col.is_some_and(|c| removed.iter().any(|r| r.eq_ignore_ascii_case(&c)))
        }

        fn reduce(expr: &Expression, removed: &[String]) -> Option<Expression> {
            match expr {
                Expression::BinaryOp { left, op: vibesql_ast::BinaryOperator::And, right } => {
                    let l = reduce(left, removed);
                    let r = reduce(right, removed);
                    match (l, r) {
                        (Some(l), Some(r)) => Some(Expression::BinaryOp {
                            left: Box::new(l),
                            op: vibesql_ast::BinaryOperator::And,
                            right: Box::new(r),
                        }),
                        (Some(e), None) | (None, Some(e)) => Some(e),
                        (None, None) => None,
                    }
                }
                Expression::Conjunction(exprs) => {
                    let kept: Vec<Expression> =
                        exprs.iter().filter_map(|e| reduce(e, removed)).collect();
                    match kept.len() {
                        0 => None,
                        1 => kept.into_iter().next(),
                        _ => Some(Expression::Conjunction(kept)),
                    }
                }
                _ if is_removed_equality(expr, removed) => None,
                other => Some(other.clone()),
            }
        }

        where_clause.and_then(|w| reduce(w, removed_columns))
    }

    /// Wrap key expressions as ASC ORDER BY items for the index-delivery
    /// checks (grouping and dedup keys have no inherent direction).
    fn exprs_as_asc_order_items(exprs: &[&Expression]) -> Vec<vibesql_ast::OrderByItem> {
        exprs
            .iter()
            .map(|e| vibesql_ast::OrderByItem {
                expr: (*e).clone(),
                direction: vibesql_ast::OrderDirection::Asc,
                nulls_order: None,
            })
            .collect()
    }

    /// True when the statement-level ORDER BY matches `exprs` exactly: same
    /// expressions (output ordinals/aliases resolved), same sequence, same
    /// length, no explicit NULLS ordering. With `require_asc`, every term
    /// must also be ASC (the DISTINCT rule); otherwise directions are
    /// ignored (the GROUP BY rule — sqlite3 suppresses for DESC and even
    /// mixed-direction matches, verified live). Bare prefixes and
    /// permutations never match (verified live).
    fn order_by_matches_exprs(
        order_by: &[vibesql_ast::OrderByItem],
        exprs: &[&Expression],
        select_list: &[SelectItem],
        require_asc: bool,
    ) -> bool {
        order_by.len() == exprs.len()
            && order_by.iter().zip(exprs).all(|(item, expr)| {
                item.nulls_order.is_none()
                    && (!require_asc || item.direction == vibesql_ast::OrderDirection::Asc)
                    && Self::resolve_output_expr(&item.expr, select_list) == *expr
            })
    }

    /// The ASC ORDER BY items under which an index delivers the rows in
    /// GROUP BY order, if any — `Some` suppresses the
    /// `USE TEMP B-TREE FOR GROUP BY` line, and the returned key becomes the
    /// base scan's ordering requirement so the scan line shows the
    /// delivering index like sqlite3 (#5371).
    ///
    /// Grouping is order-insensitive, so SQLite reorders the GROUP BY terms
    /// to match a candidate index (`GROUP BY b, a` with index `(a, b)`
    /// suppresses — verified live). We first try the key as written, then
    /// retry with the terms permuted into each index's column order.
    fn group_key_index_order(
        from: Option<&vibesql_ast::FromClause>,
        where_clause: Option<&vibesql_ast::Expression>,
        group_key: &[&Expression],
        database: &Database,
    ) -> Option<Vec<vibesql_ast::OrderByItem>> {
        Self::key_index_order(from, where_clause, group_key, database)
    }

    /// The ASC ORDER BY items under which an index delivers `key` order, trying
    /// the key as written first and then each candidate index's column-order
    /// permutation (grouping and DISTINCT are both order-insensitive, so SQLite
    /// reorders the key terms to match a covering index — `GROUP BY b, a` /
    /// `SELECT DISTINCT b, a` with index `(a, b)` both suppress, verified live).
    /// Returns `None` when no index delivers any permutation.
    fn key_index_order(
        from: Option<&vibesql_ast::FromClause>,
        where_clause: Option<&vibesql_ast::Expression>,
        key: &[&Expression],
        database: &Database,
    ) -> Option<Vec<vibesql_ast::OrderByItem>> {
        let items = Self::exprs_as_asc_order_items(key);
        if !Self::needs_temp_btree_for_order_by(from, where_clause, &items, database) {
            return Some(items);
        }

        // Order-insensitive retry: only bare column references can be
        // realigned to an index's column order.
        let Some(vibesql_ast::FromClause::Table { name, .. }) = from else {
            return None;
        };
        let col_names: Option<Vec<&str>> = key
            .iter()
            .map(|e| match e {
                Expression::ColumnRef(col_id) => Some(col_id.column_canonical()),
                _ => None,
            })
            .collect();
        let col_names = col_names?;

        for index_name in database.list_indexes_for_table(name) {
            let Some(index) = database.get_index(&index_name) else { continue };
            // Permute the key terms into this index's column order; every
            // term must appear as an index column for the realignment to be
            // meaningful.
            let positions: Option<Vec<usize>> = col_names
                .iter()
                .map(|col| {
                    index.columns.iter().position(|ic| {
                        ic.column_name().is_some_and(|n| n.eq_ignore_ascii_case(col))
                    })
                })
                .collect();
            let Some(positions) = positions else { continue };

            let mut order: Vec<usize> = (0..key.len()).collect();
            order.sort_by_key(|&i| positions[i]);
            if order.iter().zip(0..key.len()).all(|(&a, b)| a == b) {
                continue; // Same as the as-written key already checked.
            }
            let permuted: Vec<&Expression> = order.iter().map(|&i| key[i]).collect();
            let permuted_items = Self::exprs_as_asc_order_items(&permuted);
            if !Self::needs_temp_btree_for_order_by(from, where_clause, &permuted_items, database) {
                return Some(permuted_items);
            }
        }

        None
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
        let select_list_has_aggregate = Self::select_list_has_aggregate(stmt);

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

    /// True when any SELECT-list expression contains an aggregate function.
    fn select_list_has_aggregate(stmt: &SelectStmt) -> bool {
        stmt.select_list.iter().any(|item| match item {
            SelectItem::Expression { expr, .. } => contains_aggregate_function(expr),
            SelectItem::Wildcard { .. } | SelectItem::QualifiedWildcard { .. } => false,
        })
    }

    /// True when a WHERE-clause `EXISTS`/`IN` subquery cannot be folded into
    /// the outer FROM-clause join/semi-join plan by SQLite's exists-to-join
    /// optimizer, and therefore keeps its own `SUBQUERY` EQP entry instead of
    /// disappearing into a bare `SCAN <outer-table>` (#6647): a subquery
    /// whose own FROM clause is not a single table (SQLite only rewrites a
    /// single-table correlated body into a semi-join — existsexpr.test 3.9),
    /// or a subquery whose own SELECT list aggregates without a `GROUP BY`
    /// (such a subquery always returns exactly one row, so it can never
    /// become a per-outer-row join predicate — existsexpr.test 3.7).
    fn subquery_blocks_where_flattening(subquery: &SelectStmt) -> bool {
        // Deliberately "anything that is not a single base table", not
        // strictly "more than one table": a join, a derived table
        // (`FromClause::Subquery`), and a table-valued function all fall
        // outside the single-base-table shape SQLite's exists-to-join
        // rewrite handles, so each one keeps its own EQP entry. The name
        // describes the common case (a multi-table FROM), not the full set.
        let non_single_table_from = matches!(
            subquery.from.as_ref(),
            Some(f) if !matches!(f, FromClause::Table { .. })
        );
        // Narrow aggregate check on purpose (#6647): `select_list_has_aggregate`
        // is the view-flattening gate's *conservative* helper, which reports
        // "aggregate" for any nested subquery expression. Reusing it here would
        // render a spurious `CORRELATED SCALAR SUBQUERY` node for a subquery
        // whose SELECT list merely mentions another subquery, e.g.
        // `WHERE EXISTS (SELECT (SELECT 1) FROM t2 WHERE t2.b = t1.a)`.
        let aggregate_without_group_by =
            subquery.group_by.is_none() && Self::select_list_has_real_aggregate(subquery);
        non_single_table_from || aggregate_without_group_by
    }

    /// True when any SELECT-list expression contains an *actual* aggregate
    /// function call, treating nested subquery expressions as opaque (#6647).
    /// See [`contains_real_aggregate_function`] for why this is not
    /// [`Self::select_list_has_aggregate`].
    fn select_list_has_real_aggregate(stmt: &SelectStmt) -> bool {
        stmt.select_list.iter().any(|item| match item {
            SelectItem::Expression { expr, .. } => contains_real_aggregate_function(expr),
            SelectItem::Wildcard { .. } | SelectItem::QualifiedWildcard { .. } => false,
        })
    }

    /// Recursively collect the subquery bodies of WHERE-clause `EXISTS`/`IN`
    /// expressions that [`subquery_blocks_where_flattening`] identifies as
    /// un-flattenable, reachable through AND/OR/NOT boolean combinators
    /// (#6647). Other predicate shapes (a subquery nested inside a
    /// comparison, CASE, etc.) are out of scope for this issue.
    fn collect_unflattenable_where_subqueries<'a>(
        expr: &'a Expression,
        out: &mut Vec<&'a SelectStmt>,
    ) {
        match expr {
            Expression::Exists { subquery, .. } | Expression::In { subquery, .. } => {
                if Self::subquery_blocks_where_flattening(subquery) {
                    out.push(subquery);
                }
            }
            Expression::Conjunction(exprs) | Expression::Disjunction(exprs) => {
                for e in exprs {
                    Self::collect_unflattenable_where_subqueries(e, out);
                }
            }
            Expression::BinaryOp { left, right, .. } => {
                Self::collect_unflattenable_where_subqueries(left, out);
                Self::collect_unflattenable_where_subqueries(right, out);
            }
            Expression::UnaryOp { expr, .. } => {
                Self::collect_unflattenable_where_subqueries(expr, out);
            }
            _ => {}
        }
    }

    /// Collect the subqueries of `EXISTS`/`IN`/scalar-subquery expressions
    /// appearing directly in a SELECT list. Unlike a WHERE-clause predicate,
    /// SQLite never rewrites a SELECT-list subquery into a join — every one
    /// keeps its own `SUBQUERY` EQP entry (existsexpr.test 4.4, #6647).
    fn collect_select_list_subqueries(select_list: &[SelectItem]) -> Vec<&SelectStmt> {
        select_list
            .iter()
            .filter_map(|item| match item {
                SelectItem::Expression { expr, .. } => match expr {
                    Expression::Exists { subquery, .. } | Expression::In { subquery, .. } => {
                        Some(subquery.as_ref())
                    }
                    Expression::ScalarSubquery(subquery) => Some(subquery.as_ref()),
                    _ => None,
                },
                SelectItem::Wildcard { .. } | SelectItem::QualifiedWildcard { .. } => None,
            })
            .collect()
    }

    /// True when a compound body contains any deduplicating set operation
    /// (UNION/INTERSECT/EXCEPT without ALL). SQLite cannot flatten such
    /// derived tables and renders them as CO-ROUTINE blocks; UNION-ALL-only
    /// chains flatten into a top-level COMPOUND QUERY (verified live).
    fn compound_has_dedup(stmt: &SelectStmt) -> bool {
        let mut set_op = stmt.set_operation.as_ref();
        while let Some(op) = set_op {
            if !op.all {
                return true;
            }
            set_op = op.right.set_operation.as_ref();
        }
        false
    }

    /// Generate plan node for FROM clause
    ///
    /// `prefer_ordering_scan` is true when `order_by` is an ordering
    /// requirement the scan may ride an index for even without predicates —
    /// a window sort key (windowpushd.test 2.1.3.6), an index-delivered
    /// GROUP BY/DISTINCT key, or a statement-level ORDER BY whose temp-line
    /// suppression fired (#5371).
    fn explain_from_clause(
        from: &vibesql_ast::FromClause,
        where_clause: &Option<vibesql_ast::Expression>,
        order_by: &Option<Vec<vibesql_ast::OrderByItem>>,
        prefer_ordering_scan: bool,
        needed_columns: &HashSet<String>,
        database: &Database,
        ctes: &HashSet<String>,
    ) -> Result<PlanNode, ExecutorError> {
        match from {
            vibesql_ast::FromClause::Table { name, alias, index_hint, .. } => {
                // Expand views in EQP output instead of showing an opaque
                // `SCAN <view>`. CTE names shadow same-named views and are
                // never expanded.
                //
                // - Window-function views (#5347) and blocked bodies — aggregates, GROUP BY/HAVING,
                //   DISTINCT, LIMIT/OFFSET, compound, VALUES, WITH (#5361): SQLite cannot flatten
                //   them, so the body's plan renders as a CO-ROUTINE block plus a trailing `SCAN
                //   <name>` (windowpushd.test 2.1.3.6; sqlite3 3.51.0 verified per category).
                //   VibeSQL's runtime materializes every view body, so the block + inner plan is
                //   truthful even where SQLite manages to flatten a specific shape (LIMIT-only
                //   bodies, UNION ALL bodies, single-use plain CTEs — documented divergences in
                //   explain_view_expansion_tests.rs).
                // - Plain flattenable views (#5355): SQLite inlines the body into the outer query
                //   and shows the underlying table access with no mention of the view. VibeSQL's
                //   runtime MATERIALIZES views (select/scan/table.rs executes the full body, then
                //   post-filters the outer WHERE), so the inner scans shown here are the truthful
                //   access path; we deliberately do NOT fabricate SQLite's outer-WHERE push-down
                //   (`SEARCH <table> (x=?)`) because no index probe happens at runtime. Where no
                //   index applies the output matches SQLite exactly (`SCAN <table>`).
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
                    prefer_ordering_scan,
                    needed_columns,
                    database,
                    index_hint.as_ref(),
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
                // Correlated-join MULTI-INDEX OR (epic #5668, where9-3.1/3.2):
                // a 2-table (CROSS or LEFT) join whose OR join predicate
                // `(t1.c=t2.c AND t1.d=t2.d) OR t1.f=t2.f` drives a parameterized
                // MULTI-INDEX OR on the inner table. Render the outer driver line
                // plus the inner MULTI-INDEX OR subtree to match sqlite3 3.51.0.
                // The runtime already produces correct rows via nested-loop scan;
                // this only fixes the EQP access-path rendering. Returns `None`
                // (falling through to the generic join rendering) for any shape it
                // does not handle, and is suppressed by `MULTI_INDEX_OR_DISABLED`.
                if let Some(node) = Self::try_explain_correlated_join_multi_index_or(
                    left,
                    right,
                    join_type,
                    condition.as_ref(),
                    where_clause.as_ref(),
                    needed_columns,
                    database,
                )? {
                    return Ok(node);
                }

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
                    prefer_ordering_scan,
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

                // Derived tables SQLite cannot flatten render as a
                // `CO-ROUTINE <alias>` block: window functions (#5347),
                // aggregates, GROUP BY/HAVING, DISTINCT, and compounds with
                // a deduplicating set operation (#5367 — sqlite3 3.51.0
                // shows `CO-ROUTINE q` around the COMPOUND QUERY for dedup
                // UNION/INTERSECT/EXCEPT bodies, verified live). Truthful:
                // the runtime materializes derived tables. UNION-ALL-only
                // compounds, LIMIT-only, and plain bodies keep the existing
                // flat rendering, matching SQLite's flattener exactly.
                if Self::select_has_window_functions(query)
                    || query.group_by.is_some()
                    || query.distinct
                    || query.having.is_some()
                    || Self::select_list_has_aggregate(query)
                    || Self::compound_has_dedup(query)
                {
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
            vibesql_ast::FromClause::TableFunction { name, args, alias, column_aliases } => {
                let mut tvf_node = PlanNode::new("TableFunction");
                if let Some(a) = alias {
                    tvf_node.object = Some(format!("{} AS {}", name, a));
                } else {
                    tvf_node.object = Some(name.clone());
                }

                tvf_node.details.push(format!("{} arg(s)", args.len()));

                if let Some(aliases) = column_aliases {
                    tvf_node.details.push(format!("Columns: {}", aliases.join(", ")));
                }

                Ok(tvf_node)
            }
        }
    }

    /// Try to render a 2-table (CROSS or LEFT) join whose OR join predicate is a
    /// **correlated MULTI-INDEX OR** on the inner table (epic #5668,
    /// where9-3.1/3.2).
    ///
    /// Shape handled (verified against sqlite3 3.51.0):
    /// ```text
    /// SELECT ... FROM t1, t2 WHERE t1.a=80 AND ((t1.c=t2.c AND t1.d=t2.d) OR t1.f=t2.f)
    /// SELECT ... FROM t1 LEFT JOIN t2 ON (t1.c+1=t2.c AND t1.d=t2.d) OR (t1.f||'x')=t2.f WHERE t1.a=80
    /// ```
    /// SQLite drives the outer table `t1` (here by rowid, `t1.a=80`) and on the
    /// inner table `t2` performs a MULTI-INDEX OR keyed by the correlated outer
    /// values. VibeSQL's runtime already returns correct rows via nested-loop
    /// scan; this method only corrects the EQP access-path rendering.
    ///
    /// Returns `Ok(None)` (caller falls back to generic join rendering) for any
    /// shape this does not handle, and is suppressed entirely when
    /// `MULTI_INDEX_OR_DISABLED` is set.
    ///
    /// # Branch ordinals
    /// SQLite labels each branch `INDEX <n>` by an internal WHERE-term slot, not
    /// a clean 1..N over chosen branches. For the handled shape (a leading
    /// multi-equality AND branch followed by single-equality branches) the slot
    /// advances by the number of **plain `col = col`** equality conjuncts in each
    /// branch (minimum one), which reproduces where9-3.1 (`INDEX 1`/`INDEX 3`)
    /// and where9-3.2 (`INDEX 1`/`INDEX 2`) exactly. See module probes in
    /// `explain_correlated_join_or_tests.rs`.
    #[allow(clippy::too_many_arguments)]
    fn try_explain_correlated_join_multi_index_or(
        left: &vibesql_ast::FromClause,
        right: &vibesql_ast::FromClause,
        join_type: &vibesql_ast::JoinType,
        condition: Option<&Expression>,
        where_clause: Option<&Expression>,
        needed_columns: &HashSet<String>,
        database: &Database,
    ) -> Result<Option<PlanNode>, ExecutorError> {
        use crate::select::scan::index_scan::multi_index_or_enabled;

        // Kill switch: behave exactly as before the feature when disabled.
        if !multi_index_or_enabled() {
            return Ok(None);
        }

        // Only CROSS (comma) and LEFT OUTER joins are in scope.
        let is_left_join = match join_type {
            vibesql_ast::JoinType::Cross | vibesql_ast::JoinType::Inner => false,
            vibesql_ast::JoinType::LeftOuter => true,
            _ => return Ok(None),
        };

        // Both sides must be plain base tables (no nested joins/subqueries).
        let (Some((outer_name, outer_alias)), Some((inner_name, inner_alias))) =
            (Self::base_table_of(left), Self::base_table_of(right))
        else {
            return Ok(None);
        };
        let outer_ref = outer_alias.unwrap_or(outer_name);
        let inner_ref = inner_alias.unwrap_or(inner_name);

        // The correlated OR lives in the ON condition (LEFT JOIN) or, for a comma
        // join, as a top-level conjunct of the WHERE clause.
        let or_expr = if let Some(cond) = condition {
            Self::top_level_or(cond)
        } else {
            where_clause.and_then(Self::find_correlated_or_conjunct)
        };
        let Some(or_expr) = or_expr else {
            return Ok(None);
        };

        let branches = Self::or_branches_flat(or_expr);
        // Need at least two branches for a union.
        if branches.len() < 2 {
            return Ok(None);
        }

        // Inner-table columns referenced anywhere in the query: SELECT list
        // (`needed_columns`) plus every inner column constrained by the OR.
        // Used for covering-index detection per branch.
        let mut inner_needed: HashSet<String> = HashSet::new();
        for col in needed_columns {
            inner_needed.insert(col.to_lowercase());
        }
        Self::collect_table_columns(or_expr, inner_ref, inner_name, database, &mut inner_needed);

        // Build one MULTI-INDEX OR branch per OR branch. Every branch must
        // contribute at least one indexable inner-table equality constraint; if
        // any branch does not, this is not a correlated MULTI-INDEX OR.
        let mut or_branch_nodes: Vec<(usize, PlanNode)> = Vec::with_capacity(branches.len());
        let mut slot: usize = 1;
        for branch in &branches {
            // Inner-table equality constraints in this branch, in order.
            let inner_eqs = Self::inner_equality_columns(branch, inner_ref, inner_name, database);
            if inner_eqs.is_empty() {
                return Ok(None);
            }

            // Choose the index VibeSQL would use for this branch's inner
            // constraints.
            let Some(index_name) =
                Self::resolve_inner_branch_index(inner_name, &inner_eqs, database)
            else {
                return Ok(None);
            };

            let is_covering = is_covering_index(
                &index_name,
                &Self::covering_needed_columns(inner_name, &inner_needed, database),
                database,
            );
            let scan_type = if is_covering { ScanType::CoveringIndex } else { ScanType::Search };

            let mut search = PlanNode::new("Index Scan")
                .with_object(inner_name)
                .with_scan_type(scan_type)
                .with_index_name(&index_name);
            search.left_join = is_left_join;
            if let Some(leading) = Self::index_leading_column(&index_name, database) {
                search = search.with_index_predicate(&leading, "=");
            }

            or_branch_nodes.push((slot, search));

            // Advance the WHERE-term slot by the count of plain `col = col`
            // equality conjuncts in this branch (minimum one). This reproduces
            // SQLite's branch-ordinal slots for the handled shape.
            let plain_eqs = Self::plain_column_equality_count(branch);
            slot += plain_eqs.max(1);
        }

        // Require a genuine multi-index union (at least two distinct indexes).
        let distinct: HashSet<&str> =
            or_branch_nodes.iter().map(|(_, n)| n.index_name.as_deref().unwrap_or("")).collect();
        if distinct.len() < 2 {
            return Ok(None);
        }

        // Outer driver: render via the normal single-table path so `t1.a=80`
        // becomes `SEARCH t1 USING INTEGER PRIMARY KEY (rowid=?)` (or whatever
        // index the outer WHERE selects). Pass only the outer table's portion of
        // the WHERE so the OR (which references the inner table) is not applied.
        let outer_where = where_clause.and_then(|w| {
            Self::outer_only_where(w, outer_ref, outer_name, inner_ref, inner_name, database)
        });
        let empty_cols = HashSet::new();
        let mut join_node =
            PlanNode::new(if is_left_join { "Left Outer Join" } else { "Cross Join" });
        let outer_node = Self::explain_table_scan(
            outer_name,
            outer_alias,
            &outer_where,
            &None,
            false,
            &empty_cols,
            database,
            // Synthetic outer-driver rendering for correlated-join MULTI-INDEX
            // OR (#6405: INDEXED BY forcing is single-table-FROM scope only).
            None,
        )?;
        join_node.add_child(outer_node);

        // Inner: the MULTI-INDEX OR subtree.
        let mut inner_node = PlanNode::new("Multi-Index Or").with_object(inner_name);
        inner_node.multi_index_or_branches = or_branch_nodes;
        join_node.add_child(inner_node);

        Ok(Some(join_node))
    }

    /// Extract `(name, alias)` if `from` is a plain base table, else `None`.
    fn base_table_of(from: &vibesql_ast::FromClause) -> Option<(&str, Option<&str>)> {
        match from {
            vibesql_ast::FromClause::Table { name, alias, .. } => {
                Some((name.as_str(), alias.as_deref()))
            }
            _ => None,
        }
    }

    /// If `expr` is itself a top-level OR (`Disjunction` or `OR` BinaryOp),
    /// return it; otherwise `None`.
    fn top_level_or(expr: &Expression) -> Option<&Expression> {
        match expr {
            Expression::Disjunction(_) => Some(expr),
            Expression::BinaryOp { op: vibesql_ast::BinaryOperator::Or, .. } => Some(expr),
            _ => None,
        }
    }

    /// Find a top-level (AND-reachable) OR conjunct in a WHERE clause. Returns
    /// the first OR sub-expression found, or `None`.
    fn find_correlated_or_conjunct(expr: &Expression) -> Option<&Expression> {
        match expr {
            Expression::Disjunction(_)
            | Expression::BinaryOp { op: vibesql_ast::BinaryOperator::Or, .. } => Some(expr),
            Expression::Conjunction(exprs) => {
                exprs.iter().find_map(Self::find_correlated_or_conjunct)
            }
            Expression::BinaryOp { left, op: vibesql_ast::BinaryOperator::And, right } => {
                Self::find_correlated_or_conjunct(left)
                    .or_else(|| Self::find_correlated_or_conjunct(right))
            }
            _ => None,
        }
    }

    /// Flatten a top-level OR into its branch expressions.
    fn or_branches_flat(or_expr: &Expression) -> Vec<&Expression> {
        match or_expr {
            Expression::Disjunction(exprs) => exprs.iter().collect(),
            Expression::BinaryOp { op: vibesql_ast::BinaryOperator::Or, left, right } => {
                let mut v = Self::or_branches_flat(left);
                v.extend(Self::or_branches_flat(right));
                v
            }
            _ => vec![or_expr],
        }
    }

    /// Flatten a branch (a single predicate or an AND-conjunction) into its
    /// conjunct expressions.
    fn branch_conjuncts(branch: &Expression) -> Vec<&Expression> {
        match branch {
            Expression::Conjunction(exprs) => {
                exprs.iter().flat_map(Self::branch_conjuncts).collect()
            }
            Expression::BinaryOp { left, op: vibesql_ast::BinaryOperator::And, right } => {
                let mut v = Self::branch_conjuncts(left);
                v.extend(Self::branch_conjuncts(right));
                v
            }
            _ => vec![branch],
        }
    }

    /// Resolve whether a column reference belongs to the inner table (by alias
    /// or, for unqualified columns, by schema membership).
    fn column_is_inner(
        col: &vibesql_ast::ColumnIdentifier,
        inner_ref: &str,
        inner_name: &str,
        database: &Database,
    ) -> bool {
        if let Some(tbl) = col.table_canonical() {
            return tbl.eq_ignore_ascii_case(inner_ref) || tbl.eq_ignore_ascii_case(inner_name);
        }
        // Unqualified: belongs to the inner table iff that table has the column.
        database
            .get_table(inner_name)
            .map(|t| t.schema.get_column_index(col.column_canonical()).is_some())
            .unwrap_or(false)
    }

    /// The inner-table columns that appear as one side of an equality
    /// (`inner.col = <outer expr>`), in branch order. Each such column is an
    /// index seek key keyed by the correlated outer value.
    fn inner_equality_columns(
        branch: &Expression,
        inner_ref: &str,
        inner_name: &str,
        database: &Database,
    ) -> Vec<String> {
        let mut cols = Vec::new();
        for conj in Self::branch_conjuncts(branch) {
            if let Expression::BinaryOp { left, op: vibesql_ast::BinaryOperator::Equal, right } =
                conj
            {
                // Inner column on either side; the other side is the outer
                // parameter (a column ref, expression, or literal).
                if let Expression::ColumnRef(c) = left.as_ref() {
                    if Self::column_is_inner(c, inner_ref, inner_name, database) {
                        cols.push(c.column_canonical().to_string());
                        continue;
                    }
                }
                if let Expression::ColumnRef(c) = right.as_ref() {
                    if Self::column_is_inner(c, inner_ref, inner_name, database) {
                        cols.push(c.column_canonical().to_string());
                    }
                }
            }
        }
        cols
    }

    /// Count plain `col = col` equality conjuncts (both sides bare column refs)
    /// in a branch. Drives the SQLite branch-ordinal slot advance.
    fn plain_column_equality_count(branch: &Expression) -> usize {
        Self::branch_conjuncts(branch)
            .iter()
            .filter(|conj| {
                matches!(
                    conj,
                    Expression::BinaryOp {
                        left,
                        op: vibesql_ast::BinaryOperator::Equal,
                        right,
                    } if matches!(left.as_ref(), Expression::ColumnRef(_))
                        && matches!(right.as_ref(), Expression::ColumnRef(_))
                )
            })
            .count()
    }

    /// Choose the index VibeSQL would use for a branch's inner-table equality
    /// constraints. Among indexes whose **leading** column is one of the
    /// branch's equality-constrained inner columns, pick deterministically the
    /// one whose leading column is MOST selective (highest distinct count) —
    /// matching SQLite, which seeks on the most selective constrained column
    /// (where9-3.1/3.2: `c=? AND d=?` → `t2d`, because `d` has more distinct
    /// values than `c`). Ties break by the sorted index name for stability.
    /// This avoids the HashMap-iteration non-determinism the general cost-based
    /// selector exhibits on near-equal-cost indexes.
    fn resolve_inner_branch_index(
        inner_name: &str,
        inner_eq_cols: &[String],
        database: &Database,
    ) -> Option<String> {
        let constrained: HashSet<String> = inner_eq_cols.iter().map(|c| c.to_lowercase()).collect();
        let table = database.get_table(inner_name)?;

        let mut best: Option<(usize, String)> = None; // (distinct_count, index_name)
        let mut candidates: Vec<String> = database.list_indexes_for_table(inner_name);
        candidates.sort(); // deterministic iteration order
        for index_name in candidates {
            let Some(meta) = database.get_index(&index_name) else { continue };
            let Some(leading) = meta.columns.first().and_then(|c| c.column_name()) else {
                continue;
            };
            let leading_lower = leading.to_lowercase();
            if !constrained.contains(&leading_lower) {
                continue;
            }
            let distinct = Self::column_distinct_count(table, &leading_lower);
            let is_better = match &best {
                None => true,
                Some((best_distinct, _)) => distinct > *best_distinct,
            };
            if is_better {
                best = Some((distinct, index_name));
            }
        }
        best.map(|(_, name)| name)
    }

    /// Count distinct non-NULL values of `column_lower` (case-insensitive) in a
    /// table by scanning its rows. Used to pick the most selective constrained
    /// column deterministically for a correlated-join MULTI-INDEX OR branch.
    fn column_distinct_count(table: &vibesql_storage::Table, column_lower: &str) -> usize {
        let Some(col_idx) = table.schema.get_column_index(column_lower) else {
            return 0;
        };
        let mut seen: HashSet<String> = HashSet::new();
        for row in table.scan() {
            if let Some(value) = row.values.get(col_idx) {
                if !matches!(value, vibesql_types::SqlValue::Null) {
                    seen.insert(format!("{:?}", value));
                }
            }
        }
        seen.len()
    }

    /// Collect inner-table column names referenced (qualified or by membership)
    /// within an expression.
    fn collect_table_columns(
        expr: &Expression,
        inner_ref: &str,
        inner_name: &str,
        database: &Database,
        out: &mut HashSet<String>,
    ) {
        match expr {
            Expression::ColumnRef(c) => {
                if Self::column_is_inner(c, inner_ref, inner_name, database) {
                    out.insert(c.column_canonical().to_lowercase());
                }
            }
            Expression::BinaryOp { left, right, .. } => {
                Self::collect_table_columns(left, inner_ref, inner_name, database, out);
                Self::collect_table_columns(right, inner_ref, inner_name, database, out);
            }
            Expression::Conjunction(es) | Expression::Disjunction(es) => {
                for e in es {
                    Self::collect_table_columns(e, inner_ref, inner_name, database, out);
                }
            }
            Expression::UnaryOp { expr: e, .. } | Expression::IsNull { expr: e, .. } => {
                Self::collect_table_columns(e, inner_ref, inner_name, database, out);
            }
            Expression::Function { args, .. } => {
                for a in args {
                    Self::collect_table_columns(a, inner_ref, inner_name, database, out);
                }
            }
            _ => {}
        }
    }

    /// The needed-column set for inner covering-index detection: the columns the
    /// query reads from the inner table, with the rowid-alias column carried
    /// implicitly (SQLite indexes always store the rowid).
    fn covering_needed_columns(
        inner_name: &str,
        inner_needed: &HashSet<String>,
        database: &Database,
    ) -> HashSet<String> {
        let mut needed = inner_needed.clone();
        if let Some(table) = database.get_table(inner_name) {
            if let Some(rowid_idx) = table.schema.rowid_alias_column {
                if let Some(col) = table.schema.columns.get(rowid_idx) {
                    needed.remove(&col.name.to_lowercase());
                }
            }
        }
        needed
    }

    /// Extract the outer-table portion of a WHERE clause: the top-level
    /// AND-conjuncts that reference only the outer table (e.g. `t1.a=80`). The
    /// correlated OR conjunct (which references the inner table) is dropped so
    /// the outer driver renders its own access path.
    fn outer_only_where(
        where_expr: &Expression,
        outer_ref: &str,
        outer_name: &str,
        inner_ref: &str,
        inner_name: &str,
        database: &Database,
    ) -> Option<Expression> {
        let mut kept: Vec<Expression> = Vec::new();
        Self::collect_outer_conjuncts(
            where_expr, outer_ref, outer_name, inner_ref, inner_name, database, &mut kept,
        );
        match kept.len() {
            0 => None,
            1 => Some(kept.into_iter().next().unwrap()),
            _ => Some(Expression::Conjunction(kept)),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn collect_outer_conjuncts(
        expr: &Expression,
        outer_ref: &str,
        outer_name: &str,
        inner_ref: &str,
        inner_name: &str,
        database: &Database,
        out: &mut Vec<Expression>,
    ) {
        match expr {
            Expression::Conjunction(es) => {
                for e in es {
                    Self::collect_outer_conjuncts(
                        e, outer_ref, outer_name, inner_ref, inner_name, database, out,
                    );
                }
            }
            Expression::BinaryOp { left, op: vibesql_ast::BinaryOperator::And, right } => {
                Self::collect_outer_conjuncts(
                    left, outer_ref, outer_name, inner_ref, inner_name, database, out,
                );
                Self::collect_outer_conjuncts(
                    right, outer_ref, outer_name, inner_ref, inner_name, database, out,
                );
            }
            other => {
                // Keep conjuncts that reference the outer table but not the inner
                // table (so the OR, which references the inner table, is dropped).
                let mut inner_cols = HashSet::new();
                Self::collect_table_columns(
                    other,
                    inner_ref,
                    inner_name,
                    database,
                    &mut inner_cols,
                );
                if inner_cols.is_empty()
                    && Self::expr_references_table(other, outer_ref, outer_name, database)
                {
                    out.push(other.clone());
                }
            }
        }
    }

    /// True when `expr` references a column of the given table (by alias or
    /// schema membership).
    fn expr_references_table(
        expr: &Expression,
        table_ref: &str,
        table_name: &str,
        database: &Database,
    ) -> bool {
        let mut cols = HashSet::new();
        Self::collect_table_columns(expr, table_ref, table_name, database, &mut cols);
        !cols.is_empty()
    }

    /// True when `index_name` covers every column the query reads from
    /// `table_name`: all SELECT-list and WHERE columns are index columns,
    /// with the rowid-alias column carried implicitly (SQLite indexes always
    /// store the rowid — windowpushd.test 1.4). An empty `needed_columns`
    /// set means a wildcard SELECT, which can never be covering.
    fn index_covers_scan(
        table_name: &str,
        index_name: &str,
        where_clause: &Option<vibesql_ast::Expression>,
        needed_columns: &HashSet<String>,
        database: &Database,
    ) -> bool {
        if needed_columns.is_empty() {
            return false;
        }
        let mut all_needed_columns = needed_columns.clone();
        if let Some(where_expr) = where_clause {
            collect_column_refs(where_expr, &mut all_needed_columns);
        }
        if let Some(table) = database.get_table(table_name) {
            if let Some(rowid_idx) = table.schema.rowid_alias_column {
                if let Some(col) = table.schema.columns.get(rowid_idx) {
                    all_needed_columns.remove(&col.name.to_lowercase());
                }
            }
        }
        is_covering_index(index_name, &all_needed_columns, database)
    }

    /// Build the `(index_name, sorted_columns)` pair `explain_table_scan` uses
    /// to render an `INDEXED BY <index_name>`-forced index scan (issue #6405),
    /// in the same shape `cost_based_index_selection` returns for a
    /// cost-model-chosen index. Returns `None` if `index_name` does not
    /// resolve to a real index (should not happen — the hint was already
    /// validated), so the caller can fall back to normal selection.
    ///
    /// Expression indexes have no column name to report, so `sorted_columns`
    /// is `None` for them — same as the runtime's `forced_index_scan_choice`
    /// in `select/scan/index_scan/selection.rs`, which this mirrors.
    fn forced_index_info(
        index_name: &str,
        database: &Database,
    ) -> Option<(String, Option<Vec<(String, vibesql_ast::OrderDirection)>>)> {
        let index_metadata = database.get_index(index_name)?;
        let sorted_columns = if index_metadata.columns.iter().any(|col| col.is_expression()) {
            None
        } else {
            Some(
                index_metadata
                    .columns
                    .iter()
                    .filter_map(|col| {
                        col.column_name().map(|name| (name.to_string(), col.direction()))
                    })
                    .collect::<Vec<_>>(),
            )
        };
        Some((index_name.to_string(), sorted_columns))
    }

    /// Generate plan node for table scan (sequential or index)
    fn explain_table_scan(
        table_name: &str,
        alias: Option<&str>,
        where_clause: &Option<vibesql_ast::Expression>,
        order_by: &Option<Vec<vibesql_ast::OrderByItem>>,
        prefer_ordering_scan: bool,
        needed_columns: &HashSet<String>,
        database: &Database,
        index_hint: Option<&vibesql_ast::IndexHint>,
    ) -> Result<PlanNode, ExecutorError> {
        // MULTI-INDEX OR (epic #5668): the runtime may execute this WHERE as a
        // union of per-branch index lookups. EQP must render the same plan the
        // runtime chooses, so we consult the actual runtime selector
        // (`select_index_scan_method`) — not just `cost_based_index_selection`,
        // which only ever returns a single index. When the selector picks
        // MULTI-INDEX OR, build SQLite's subtree and return early; otherwise fall
        // through to the existing single-scan rendering below (byte-identical).
        if order_by.is_none() {
            if let Some(IndexScanChoice::MultiIndexOr { branches, .. }) = select_index_scan_method(
                table_name,
                where_clause.as_ref(),
                None,
                database,
                index_hint,
            )? {
                let mut node = PlanNode::new("Multi-Index Or").with_object(table_name);
                for branch in &branches {
                    // Inner SEARCH line: `SEARCH <table> USING [COVERING ]INDEX
                    // <idx> (<col>=?)`. SQLite renders every branch — including an
                    // `IS NULL` branch — as a `<col>=?` equality seek on the
                    // index's leading column, so we emit `=` uniformly.
                    let is_covering = Self::index_covers_scan(
                        table_name,
                        &branch.index_name,
                        where_clause,
                        needed_columns,
                        database,
                    );
                    let scan_type =
                        if is_covering { ScanType::CoveringIndex } else { ScanType::Search };
                    let mut search = PlanNode::new("Index Scan")
                        .with_object(table_name)
                        .with_scan_type(scan_type)
                        .with_index_name(&branch.index_name);
                    if let Some(leading) = Self::index_leading_column(&branch.index_name, database)
                    {
                        search = search.with_index_predicate(&leading, "=");
                    }
                    node.multi_index_or_branches.push((branch.ordinal, search));
                }
                if let Some(table) = database.get_table(table_name) {
                    node = node.with_estimated_rows(table.row_count() as f64);
                }
                if let Some(a) = alias {
                    node.details.push(format!("Alias: {}", a));
                }
                return Ok(node);
            }
        }

        // SQLite `INDEXED BY <name>` (issue #6405): forces EQP to show the
        // named index (`SEARCH`/`SCAN ... USING INDEX <name>`) instead of the
        // cost model's normal pick, mirroring the runtime forcing in
        // `select_index_scan_method`. Falls through to the normal cost-based
        // pick if the hint doesn't resolve (defensive; should not happen —
        // `validate_index_hints` runs before EXPLAIN reaches here).
        let forced_index_info = match index_hint {
            Some(vibesql_ast::IndexHint::IndexedBy(index_name)) => {
                if let Some(info) = Self::forced_index_info(index_name, database) {
                    // Same partial-index guard as the runtime forcing in
                    // `select_index_scan_method` (issue #6405): a forced
                    // partial index whose predicate the query WHERE doesn't
                    // imply cannot be satisfied, and SQLite reports this as a
                    // prepare-time error rather than rendering a (misleading)
                    // plan for it.
                    if !crate::optimizer::predicate_implication::partial_index_usable(
                        database,
                        index_name,
                        where_clause.as_ref(),
                    ) {
                        return Err(ExecutorError::Other("no query solution".to_string()));
                    }
                    Some(info)
                } else {
                    None
                }
            }
            _ => None,
        };

        // First check for regular index scan
        let index_info = forced_index_info.or_else(|| {
            cost_based_index_selection(
                table_name,
                where_clause.as_ref(),
                order_by.as_ref().map(|v| v.as_slice()),
                database,
            )
        });

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
            // Check if this is a covering index (all needed columns are in
            // the index) — see `index_covers_scan` for the rules.
            let is_covering = Self::index_covers_scan(
                table_name,
                &index_name,
                where_clause,
                needed_columns,
                database,
            );

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
            // Pure ordering scans (no predicates) render `SCAN t USING
            // COVERING INDEX i` when the index covers all read columns,
            // matching sqlite3 (#5371). SEARCH paths carry covering via
            // `ScanType::CoveringIndex` instead.
            idx_node.index_covering = is_covering;
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
            order_by.as_deref().filter(|_| prefer_ordering_scan).and_then(|items| {
                eqp_ordering_index(table_name, where_clause.as_ref(), items, database, true)
            })
        {
            // No filtering index, but the scan feeds a pass whose key an
            // index delivers — a window sorting pass (windowpushd.test
            // 2.1.3.6: `SCAN t1 USING INDEX i2`, chosen purely for PARTITION
            // BY order), or a GROUP BY/DISTINCT/ORDER BY whose temp-line
            // suppression fired (#5371): SQLite scans that index instead of
            // sorting, rendering COVERING when it covers all read columns.
            let mut idx_node = PlanNode::new("Index Scan")
                .with_object(table_name)
                .with_scan_type(ScanType::Scan)
                .with_index_name(&ordering_index);
            idx_node.index_covering = Self::index_covers_scan(
                table_name,
                &ordering_index,
                where_clause,
                needed_columns,
                database,
            );
            idx_node
        } else if is_pk_lookup && Self::is_rowid_equality_lookup(table_name, where_clause, database)
        {
            // No regular/skip/ordering index applies, but the WHERE clause has a
            // top-level equality on the single-column INTEGER PRIMARY KEY (rowid
            // alias). The runtime resolves this as an O(1) rowid point lookup
            // (`try_primary_key_lookup` in select/scan/table.rs), so EQP renders
            // SQLite's `SEARCH <t> USING INTEGER PRIMARY KEY (rowid=?)` rather
            // than `SCAN <t>`. This is the outer-driver line for the
            // correlated-join MULTI-INDEX OR cases (where9-3.1/3.2), and also
            // matches sqlite3 3.51.0 for a bare `WHERE pk = ?` single-table scan.
            PlanNode::new("Index Scan")
                .with_object(table_name)
                .with_scan_type(ScanType::IntegerPrimaryKey)
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

    /// Leading (first) indexed column name of `index_name`, if any.
    ///
    /// Used to render each MULTI-INDEX OR branch's inner `SEARCH ... (<col>=?)`
    /// line: SQLite shows the leading index column with an `=` seek for every
    /// branch (including `IS NULL` branches).
    fn index_leading_column(index_name: &str, database: &Database) -> Option<String> {
        database
            .get_index(index_name)
            .and_then(|meta| meta.columns.first())
            .and_then(|col| col.column_name())
            .map(|s| s.to_string())
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

    /// True when the WHERE clause carries a **top-level equality** on the
    /// single-column INTEGER PRIMARY KEY (rowid alias) — the precise shape the
    /// runtime resolves as an O(1) rowid point lookup
    /// (`try_primary_key_lookup` / `extract_primary_key_values` in
    /// select/scan/table.rs).
    ///
    /// Distinct from (and stricter than) [`is_primary_key_lookup`], which only
    /// asks whether the PK column is *referenced anywhere* (used to upgrade an
    /// already-chosen index scan to the PK rendering). This stricter check
    /// gates the EQP fallback that emits `SEARCH <t> USING INTEGER PRIMARY KEY
    /// (rowid=?)` when no other index applies: it must mirror the runtime so
    /// EQP never claims a rowid seek the executor would not perform (e.g. for
    /// `WHERE pk > ?` or `WHERE pk = ? OR ...`, where the equality is not a
    /// top-level AND-conjunct).
    fn is_rowid_equality_lookup(
        table_name: &str,
        where_clause: &Option<Expression>,
        database: &Database,
    ) -> bool {
        let Some(table) = database.get_table(table_name) else {
            return false;
        };
        // Must be a single-column INTEGER PRIMARY KEY (rowid alias).
        if table.schema.rowid_alias_column.is_none() {
            return false;
        }
        let Some(pk_columns) = table.schema.primary_key.as_ref() else {
            return false;
        };
        if pk_columns.len() != 1 {
            return false;
        }
        let pk_col_lower = pk_columns[0].to_lowercase();
        let Some(where_expr) = where_clause else {
            return false;
        };
        top_level_equality_on_column(where_expr, &pk_col_lower)
    }
}

/// Recursively check if an expression contains aggregate functions.
///
/// Used by the EQP view-flattening gate (#5355): a view body whose SELECT
/// list aggregates cannot be flattened. Subqueries are conservatively treated
/// as containing aggregates (blocking flattening keeps the safe opaque
/// rendering).
///
/// **Do not reuse this for "does this SELECT list really aggregate?"** — the
/// subquery conservatism makes it answer "yes" for a SELECT list whose only
/// subquery-shaped expression contains no aggregate at all. Use
/// [`contains_real_aggregate_function`] for that question (#6647).
fn contains_aggregate_function(expr: &Expression) -> bool {
    contains_aggregate_function_inner(expr, true)
}

/// Recursively check if an expression contains an *actual* aggregate function
/// call, without [`contains_aggregate_function`]'s "any nested subquery counts
/// as an aggregate" conservatism (#6647).
///
/// A nested `ScalarSubquery`/`IN (SELECT …)`/`EXISTS (SELECT …)` is opaque
/// here: whatever it aggregates belongs to *its* result set, not to the
/// enclosing SELECT list, so it does not make the enclosing query an aggregate
/// query. This is the narrow check
/// [`ExplainExecutor::subquery_blocks_where_flattening`] needs — reusing the
/// conservative helper there would render a spurious
/// `CORRELATED SCALAR SUBQUERY` node for every WHERE-clause `EXISTS`/`IN`
/// whose own SELECT list merely mentions another subquery.
fn contains_real_aggregate_function(expr: &Expression) -> bool {
    contains_aggregate_function_inner(expr, false)
}

/// Shared walker for [`contains_aggregate_function`] (conservative) and
/// [`contains_real_aggregate_function`] (narrow). The two differ only in how
/// they answer for a nested subquery expression, so they share one traversal
/// and cannot drift apart as new `Expression` variants are handled.
fn contains_aggregate_function_inner(expr: &Expression, subquery_is_aggregate: bool) -> bool {
    let recurse = |e: &Expression| contains_aggregate_function_inner(e, subquery_is_aggregate);
    match expr {
        Expression::AggregateFunction { .. } => true,
        Expression::Function { args, .. } => args.iter().any(recurse),
        Expression::BinaryOp { left, right, .. } => recurse(left) || recurse(right),
        Expression::UnaryOp { expr, .. } => recurse(expr),
        Expression::IsNull { expr, .. } => recurse(expr),
        Expression::Cast { expr, .. } => recurse(expr),
        Expression::Conjunction(exprs) | Expression::Disjunction(exprs) => {
            exprs.iter().any(recurse)
        }
        Expression::Case { operand, when_clauses, else_result, .. } => {
            operand.as_ref().is_some_and(|e| recurse(e))
                || when_clauses
                    .iter()
                    .any(|clause| clause.conditions.iter().any(recurse) || recurse(&clause.result))
                || else_result.as_ref().is_some_and(|e| recurse(e))
        }
        Expression::InList { expr, values, .. } => recurse(expr) || values.iter().any(recurse),
        Expression::Between { expr, low, high, .. } => {
            recurse(expr) || recurse(low) || recurse(high)
        }
        // Conservative caller: subqueries may contain anything; block
        // flattening. Narrow caller: a nested subquery's aggregates belong to
        // that subquery, not to this SELECT list (#6647).
        Expression::ScalarSubquery(_) | Expression::In { .. } | Expression::Exists { .. } => {
            subquery_is_aggregate
        }
        _ => false,
    }
}

/// True when `expr` contains a top-level (AND-reachable) equality `column = ?`
/// or `? = column`, where `column` is a bare column reference (case-insensitive)
/// and the other side is not also that column.
///
/// Mirrors the runtime's `collect_equality_predicates_recursive`
/// (select/scan/table.rs): only equalities reachable through top-level `AND`
/// conjuncts qualify — an equality buried under an `OR` does not, because the
/// runtime cannot turn it into a single rowid seek.
fn top_level_equality_on_column(expr: &Expression, column: &str) -> bool {
    match expr {
        Expression::BinaryOp { left, op: vibesql_ast::BinaryOperator::Equal, right } => {
            let left_is = matches!(
                left.as_ref(),
                Expression::ColumnRef(c) if c.column_canonical().eq_ignore_ascii_case(column)
            );
            let right_is = matches!(
                right.as_ref(),
                Expression::ColumnRef(c) if c.column_canonical().eq_ignore_ascii_case(column)
            );
            // Exactly one side must be the rowid column (a literal/parameter on
            // the other side). `col = col` self-equality is not a seek.
            left_is ^ right_is
        }
        Expression::BinaryOp { left, op: vibesql_ast::BinaryOperator::And, right } => {
            top_level_equality_on_column(left, column)
                || top_level_equality_on_column(right, column)
        }
        Expression::Conjunction(exprs) => {
            exprs.iter().any(|e| top_level_equality_on_column(e, column))
        }
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
            // `count(*)` parses with a `*` pseudo-column argument; it reads
            // no actual column and must not defeat covering-index detection
            // (sqlite3 renders `SCAN t USING COVERING INDEX i` for
            // `SELECT x, count(*) ... GROUP BY x`, verified live — #5371).
            if col_id.column_canonical() != "*" {
                columns.insert(col_id.column_canonical().to_lowercase());
            }
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
    use vibesql_ast::{pretty_print::ToSql, BinaryOperator};

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
